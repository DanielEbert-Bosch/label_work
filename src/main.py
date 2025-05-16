from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Integer, String, func, Boolean, not_, and_, or_
from sqlalchemy.orm import sessionmaker, mapped_column, Mapped, Session, DeclarativeBase
import uvicorn
import sys
import os
from dotenv import load_dotenv
import time
import sqlite3
import datetime
from fastapi.responses import FileResponse
from urllib.parse import quote
from fastapi.staticfiles import StaticFiles
import json
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
from datetime import timedelta
import re

sys.path.append(os.path.dirname(__file__))

load_dotenv()

app = FastAPI()

PROD = os.getenv('PROD', '0') == '1'
SKIP_DB_CREATE = os.getenv('SKIP_DB_CREATE', '0') == '1'

# Database setup
DATABASE_FILEPATH = '/db/prod/label_work.db' if PROD else '/db/test/label_work.db'
engine = create_engine(f'sqlite://{DATABASE_FILEPATH}')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Base(DeclarativeBase):
    pass

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class LabelTask(Base):
    __tablename__ = 'label_tasks'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    fmc_id: Mapped[str] = mapped_column(String, index=True)
    measurement_checksum: Mapped[str] = mapped_column(String, unique=True, index=True)
    fmc_data: Mapped[str] = mapped_column(String)
    sia_meas_id_path: Mapped[str] = mapped_column(String)

    campaign: Mapped[str | None] = mapped_column(String, nullable=True, default=None, index=True)

    # 0 means not sent out yet
    sent_label_request_at_epoch: Mapped[int] = mapped_column(Integer, default=0)
    last_labeler: Mapped[str | None] = mapped_column(String, nullable=True, default=None)

    is_labeled: Mapped[bool] = mapped_column(Boolean, default=False)
    label_bolf_path: Mapped[str | None] = mapped_column(String, nullable=True, default=None)
    label_bolf_timestamp: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    # 0 when no relabel requested
    relabel_requested_timestamp: Mapped[int] = mapped_column(Integer, default=0)

    created_at_epoch: Mapped[int] = mapped_column(Integer)


class SkippedTaskCreate(BaseModel):
    sia_link: str
    skip_reason: str


class SkippedTask(Base):
    __tablename__ = 'skipped_tasks'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    # raw content pasted into text field
    sia_link: Mapped[str] = mapped_column(String, index=True)
    skip_reason: Mapped[str] = mapped_column(String, index=True)
    measurement_checksum: Mapped[str | None] = mapped_column(String, index=True, nullable=True, default=None)
    reviewed: Mapped[bool] = mapped_column(Boolean, default=False)


class FmcBlacklisted(Base):
    __tablename__ = 'fmc_blacklist'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    measurement_checksum: Mapped[str] = mapped_column(String, unique=True, index=True)


class Metric(Base):
    __tablename__ = 'metrics_history'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    created_at_epoch: Mapped[int] = mapped_column(Integer)

    total_labelable: Mapped[int] = mapped_column(Integer)
    labeled: Mapped[int] = mapped_column(Integer)
    not_labeled: Mapped[int] = mapped_column(Integer)
    opened: Mapped[int] = mapped_column(Integer)
    opened_pending: Mapped[int] = mapped_column(Integer)


class Score(Base):
    __tablename__ = 'scores'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    labeler: Mapped[str] = mapped_column(String, unique=True, index=True)
    score: Mapped[int] = mapped_column(Integer, default=0)


class LabelTaskCreate(BaseModel):
    fmc_id: str
    fmc_data: str
    measurement_checksum: str
    sia_meas_id_path: str


class LabeledTask(BaseModel):
    measurement_checksum: str
    label_bolf_path: str


class RelabelTaskRequest(BaseModel):
    fmc_sequence_id: str

ALLOWED_REASON = [
    "obstacle_height_too_high",
    "obstacle_height_too_low",
    "wrong_metadata"
]

@app.get('/api/get_task')
async def get_task(labeler_name: str, campaign: str, db: Session = Depends(get_db)):
    current_time_epoch = int(time.time())
    filter_backlisted = (db.query(SkippedTask).filter(and_(SkippedTask.measurement_checksum == LabelTask.measurement_checksum, not_(SkippedTask.skip_reason.in_(ALLOWED_REASON)))).exists())
    filter_fmc_backlisted = db.query(FmcBlacklisted).filter(FmcBlacklisted.measurement_checksum == LabelTask.measurement_checksum).exists()
    if campaign.lower() == 'any':
        db_task = db.query(LabelTask).filter(LabelTask.is_labeled == False).filter(not_(filter_backlisted)).filter(not_(filter_fmc_backlisted)).filter((current_time_epoch - 60 * 60 * 24 * 3) > LabelTask.sent_label_request_at_epoch).order_by(func.random()).first()
    else:
        db_task = db.query(LabelTask).filter(LabelTask.is_labeled == False).filter(not_(filter_backlisted)).filter(not_(filter_fmc_backlisted)).filter((current_time_epoch - 60 * 60 * 24 * 3) > LabelTask.sent_label_request_at_epoch).filter(LabelTask.campaign == campaign).order_by(func.random()).first()

    if not db_task:
        return {'finished': True}

    db_task.last_labeler = labeler_name
    db_task.sent_label_request_at_epoch = current_time_epoch

    encoded_name = quote(labeler_name)

    sia_url = f'https://dev.sia.bosch-automotive-mlops.com/?time=0&minimalLabelMode=true&minimalLabelModeUsername={encoded_name}&measId={db_task.sia_meas_id_path}'

    db.commit()
    db.refresh(db_task)

    db_score = db.query(Score).filter(Score.labeler == labeler_name).first()
    if not db_score:
        db_score = Score(labeler=labeler_name, score=1)
        db.add(db_score)
    else:
        db_score.score += 1
    db.commit()

    return {'task': db_task, 'sia_url': sia_url}


@app.get('/api/get_label_campaigns')
def get_label_campaigns(db: Session = Depends(get_db)):
    return [row[0] for row in db.query(LabelTask.campaign).filter(LabelTask.campaign != None).distinct()]


@app.get('/api/get_open_tasks')
async def get_open_tasks(db: Session = Depends(get_db)):
    current_time_epoch = int(time.time())
    filter_backlisted = db.query(SkippedTask).filter(SkippedTask.measurement_checksum == LabelTask.measurement_checksum).exists()
    filter_fmc_backlisted = db.query(FmcBlacklisted).filter(FmcBlacklisted.measurement_checksum == LabelTask.measurement_checksum).exists()
    return db.query(LabelTask).filter(LabelTask.is_labeled == False).filter(not_(filter_backlisted)).filter(not_(filter_fmc_backlisted)).filter((current_time_epoch - 60 * 60 * 24 * 1) > LabelTask.sent_label_request_at_epoch).order_by(func.random()).all()


@app.get('/api/test_remove_realworld')
async def test_remove(db: Session = Depends(get_db)):
    db_tasks = db.query(LabelTask).all()

    cutoff_epoch = int(datetime.datetime.fromisoformat('2025-02-10T10:16:35.000Z'.replace('Z', '+00:00')).timestamp())

    ids_to_delete = []

    for task in db_tasks:
        fmc_data_dict = json.loads(task.fmc_data)
        if type(fmc_data_dict) != dict:
            continue
        if 'recordingDate' not in fmc_data_dict:
            continue
        recDate = fmc_data_dict['recordingDate']
        recEpoch = int(datetime.datetime.fromisoformat(recDate.replace('Z', '+00:00')).timestamp())
        if recEpoch > cutoff_epoch:
            ids_to_delete.append(task.id)
            print(recEpoch, cutoff_epoch)

    del_count = 0

    for id_val in ids_to_delete:
        row_to_delete = db.query(LabelTask).filter(LabelTask.id == id_val).first()
        if row_to_delete:
            db.delete(row_to_delete)
            del_count += 1

    db.commit()

    return {'del_count': del_count, 'sum': len(ids_to_delete)}


bolf_path_url_regex = re.compile(r'^.*\/(?P<date>[^\/]+)\/[^\/]+\.json$')


def get_bolf_timestamp(path: str) -> int | None:
    match = bolf_path_url_regex.match(path)

    if not match:
        print('Failed to get date from', path)
        return None

    return int(datetime.datetime.strptime(match.group('date'), '%Y_%m_%d_%H_%M_%S').timestamp())


@app.post('/api/set_labeled')
async def set_labeled(tasks: list[LabeledTask], db: Session = Depends(get_db)):
    if not tasks:
        raise HTTPException(status_code=400, detail='No tasks provided')

    print(f'set label request with {len(tasks)} tasks')

    created_labeled_tasks = []

    for task in tasks:
        db_task = db.query(LabelTask).filter(LabelTask.measurement_checksum == task.measurement_checksum).first()
        if not db_task:
            print(f'Unknown task {task}')
            continue

        try:
            label_bolf_timestamp = get_bolf_timestamp(task.label_bolf_path)
        except Exception as e:
            print(f'Failed to get bolf timestamp for {task.label_bolf_path}, {e}')

        if label_bolf_timestamp < db_task.relabel_requested_timestamp:
            # relabel requested
            continue

        db_task.label_bolf_timestamp = label_bolf_timestamp

        db_task.label_bolf_path = task.label_bolf_path
        db_task.is_labeled = True
        db.commit()

        created_labeled_tasks.append(task.model_dump())

    return created_labeled_tasks


@app.post('/api/request_relabel')
async def request_relabel(tasks: list[RelabelTaskRequest], db: Session = Depends(get_db)):
    current_time_epoch = int(time.time())

    if not tasks:
        raise HTTPException(status_code=400, detail='No tasks provided')

    relabel_requested = []

    for task in tasks:
        db_task = db.query(LabelTask).filter(LabelTask.fmc_id == task.fmc_sequence_id).first()
        if not db_task:
            print(f'Unknown task {task}')
            continue

        db_task.relabel_requested_timestamp = current_time_epoch
        db_task.sent_label_request_at_epoch = 0
        db_task.last_labeler = None
        db_task.is_labeled = False
        db_task.label_bolf_path = None
        db_task.label_bolf_timestamp = None
        db.commit()

        relabel_requested.append(task.model_dump())

    return relabel_requested


@app.post('/api/add_tasks')
async def add_tasks(tasks: list[LabelTaskCreate], db: Session = Depends(get_db)):
    if not tasks:
        raise HTTPException(status_code=400, detail='No tasks provided')

    current_time = int(time.time())
    created_tasks = []

    for task in tasks:
        # TODO: check if fmc_data has correct data fields for later
        if not task.fmc_id or not task.fmc_data or not task.measurement_checksum:
            raise HTTPException(status_code=400, detail=f'Invalid Task {task}')

        db_task = db.query(LabelTask).filter(LabelTask.measurement_checksum == task.measurement_checksum).first()
        if db_task:
            print(f'Skipping Task {task}, already in db.')
            continue

        campaign = None
        try:
            campaign = json.loads(task.fmc_data)['car']['licensePlate']
        except Exception as e:
            print(f'Failed to set campaign for {task}, {e}')

        db_task = LabelTask(
            fmc_id=task.fmc_id,
            fmc_data=task.fmc_data,
            measurement_checksum=task.measurement_checksum,
            sia_meas_id_path=task.sia_meas_id_path,
            campaign=campaign,
            created_at_epoch=current_time
        )

        db.add(db_task)
        db.commit()
        created_tasks.append(task.model_dump())

    return {'created_tasks': created_tasks}


def checksum_from_sia_url(sia_link: str):
    try:
        return parse_qs(urlparse(sia_link).query)['measId'][0].split('/')[3]
    except Exception as e:
        print(f'Cannot get checksum for {sia_link}, {e}')
        return None


@app.post('/api/skip_task')
async def set_skipped(skipped_task: SkippedTaskCreate, db: Session = Depends(get_db)):
    if not skipped_task.sia_link:
        raise HTTPException(status_code=400, detail=f'Invalid sia_link {skipped_task.sia_link}')
    skip_task = SkippedTask(sia_link=skipped_task.sia_link, skip_reason=skipped_task.skip_reason, measurement_checksum=checksum_from_sia_url(skipped_task.sia_link))
    db.add(skip_task)
    db.commit()
    db.refresh(skip_task)
    return skip_task


@app.post('/api/set_fmc_blacklist')
async def set_fmc_blacklist(blacklisted_fmc_meas_checksums: list[str], db: Session = Depends(get_db)):
    db.query(FmcBlacklisted).delete()

    new_blacklist = []
    for checksum in blacklisted_fmc_meas_checksums:
        new_blacklist.append(FmcBlacklisted(measurement_checksum=checksum))

        skipped_task = db.query(SkippedTask).filter(SkippedTask.measurement_checksum == checksum).first()
        if skipped_task:
            skipped_task.reviewed = True

    db.add_all(new_blacklist)
    db.commit()
    return


@app.get('/api/get_fmc_blacklist')
async def get_fmc_blacklist(db: Session = Depends(get_db)):
    return db.query(FmcBlacklisted).all()


@app.get('/api/get_labeltool_blacklist')
async def get_labeltool_blacklist(db: Session = Depends(get_db)):
    return db.query(SkippedTask).all()


@app.get('/api/labeled_tasks')
async def labeled_tasks(db: Session = Depends(get_db)):
    tasks = db.query(LabelTask).filter(LabelTask.is_labeled == True).all()
    out = []
    for task in tasks:
        out.append({'fmc_id': task.fmc_id, 'checksum': task.measurement_checksum, 'bolf_url': task.label_bolf_path})

    return out


@app.get('/api/blacklist')
async def get_blacklist(db: Session = Depends(get_db)):
    blacklisted_fmc_ids = db.query(LabelTask.fmc_id).join(
        SkippedTask,
        LabelTask.measurement_checksum == SkippedTask.measurement_checksum
    ).filter(
        SkippedTask.measurement_checksum.isnot(None)
    ).all()

    fmc_ids = [result[0] for result in blacklisted_fmc_ids]

    return {'sequences': fmc_ids}


@app.get('/api/daniel_set_campaign_for_existing_measurements')
async def daniel_set_campaign_for_existing_measurements(db: Session = Depends(get_db)):
    for task in db.query(LabelTask):
        if task.campaign != None:
            continue
        try:
            task.campaign = json.loads(task.fmc_data)['car']['licensePlate']
        except Exception as e:
            print(f'Failed to set campaign for {task.id}, {e}')
    db.commit()


@app.get('/api/backup_db')
async def backup_db():
    current_time = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    if PROD:
        backup_path = f'/db/prod/backup/label_work_{current_time}.db'
    else:
        backup_path = f'/db/test/backup/label_work_{current_time}.db'

    source_con = sqlite3.connect(DATABASE_FILEPATH)
    backup_con = sqlite3.connect(backup_path)

    with backup_con:
        source_con.backup(backup_con)

    backup_con.close()
    source_con.close()


@app.get('/api/metrics')
async def get_metrics(db: Session = Depends(get_db)):
    filter_backlisted = (db.query(SkippedTask).filter(and_(SkippedTask.measurement_checksum == LabelTask.measurement_checksum, not_(SkippedTask.skip_reason.in_(ALLOWED_REASON)))).exists())
    filter_fmc_backlisted = db.query(FmcBlacklisted).filter(FmcBlacklisted.measurement_checksum == LabelTask.measurement_checksum).exists()
    total_count = db.query(LabelTask).filter(not_(filter_backlisted)).filter(not_(filter_fmc_backlisted)).count()
    labeled = db.query(LabelTask).filter(LabelTask.is_labeled == True).filter(not_(filter_backlisted)).filter(not_(filter_fmc_backlisted)).count()
    not_labeled = db.query(LabelTask).filter(LabelTask.is_labeled == False).filter(not_(filter_backlisted)).filter(not_(filter_fmc_backlisted)).count()
    opened = db.query(LabelTask).filter(LabelTask.sent_label_request_at_epoch != 0).filter(not_(filter_backlisted)).filter(not_(filter_fmc_backlisted)).count()
    blacklisted = db.query(LabelTask).filter(or_(filter_backlisted,filter_fmc_backlisted)).count()

    opened_pending = opened - labeled

    metrics = {
        'total_labelable': total_count,
        'labeled': labeled,
        'not_labeled': not_labeled - opened_pending,
        'opened': opened,
        'opened_pending': opened_pending,
        'blacklisted': blacklisted
    }

    metric = Metric(**metrics, created_at_epoch=int(time.time()))
    db.add(metric)
    db.commit()

    return metrics

@app.get('/api/metrics_with_sequences')
async def get_metrics(db: Session = Depends(get_db)):
    total = {row[0] for row in db.query(LabelTask.fmc_id).all()}
    labeled = {row[0] for row in db.query(LabelTask.fmc_id).filter(LabelTask.is_labeled == True).all()}
    not_labeled = {row[0] for row in db.query(LabelTask.fmc_id).filter(LabelTask.is_labeled == False).all()}
    opened = {row[0] for row in db.query(LabelTask.fmc_id).filter(LabelTask.sent_label_request_at_epoch != 0).all()}

    opened_pending = opened - labeled

    metrics_seq = {
        'total_labelable': list(total),
        'labeled': list(labeled),
        'not_labeled': list(not_labeled - opened_pending),
        'opened': list(opened),
        'opened_pending': list(opened_pending)
    }

    return metrics_seq

@app.get('/api/metrics_history')
async def get_metrics_history(db: Session = Depends(get_db)):
    return db.query(Metric).order_by(Metric.created_at_epoch).all()

@app.get('/api/leaderboard')
async def leaderboard(db: Session = Depends(get_db)):
    leaderboard_data = db.query(Score).all()
    leaderboard = defaultdict(int)
    for user in leaderboard_data:
        leaderboard[user.labeler.lower()] += user.score

    sorted_leaderboard_items = sorted(leaderboard.items(), key=lambda item: item[1], reverse=True)
    top_20_items = sorted_leaderboard_items[:20]
    return {labeler: count for labeler, count in top_20_items}

def start_of_this_week():
    now = datetime.datetime.now()
    return (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

def start_of_this_month():
    now = datetime.datetime.now()
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def compute_leaderboard_since(db: Session, since):
    tasks = db.query(LabelTask).filter(LabelTask.sent_label_request_at_epoch > 0).all()
    leaderboard = defaultdict(int)

    for task in tasks:
        if task.last_labeler:
            dt = datetime.datetime.fromtimestamp(task.sent_label_request_at_epoch)
            if dt >= since:
                leaderboard[task.last_labeler.lower()] += 1

    sorted_leaderboard_items = sorted(leaderboard.items(), key=lambda item: item[1], reverse=True)
    top_20_items = sorted_leaderboard_items[:20]
    return {labeler: count for labeler, count in top_20_items}

@app.get('/api/leaderboard_week')
async def leaderboard_week(db: Session = Depends(get_db)):
    return compute_leaderboard_since(db, start_of_this_week())

@app.get('/api/leaderboard_month')
async def leaderboard_month(db: Session = Depends(get_db)):
    return compute_leaderboard_since(db, start_of_this_month())

@app.get('/api/roland_special')
async def roland_special(db: Session = Depends(get_db)):
    leaderboard_data = db.query(Score).all()
    leaderboard = defaultdict(int)
    for user in leaderboard_data:
        leaderboard[user.labeler.lower()] += user.score

    sorted_leaderboard_items = sorted(leaderboard.items(), key=lambda item: item[1], reverse=True)
    return {labeler: count for labeler, count in sorted_leaderboard_items}


@app.get('/')
async def get_index_html():
    return FileResponse('/static/index.html', headers={'Cache-Control': 'no-cache'})

@app.get('/leaderboard')
async def get_leaderboard_html():
    return FileResponse('/static/leaderboard.html', headers={'Cache-Control': 'no-cache'})

@app.get('/map')
async def get_map(labeler_name: str, level: int):
    level_to_map_id = max(0, min(level // 4, 6))
    return FileResponse(f'/static/img/stage{level_to_map_id}.png')

app.mount('/examples', StaticFiles(directory='/static/img/examples/'), name='examples')

if not SKIP_DB_CREATE:
    Base.metadata.create_all(engine)

if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=7100, reload=True)
