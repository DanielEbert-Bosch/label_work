from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Integer, String, func, Boolean
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
from fastapi import FastAPI, Response

sys.path.append(os.path.dirname(__file__))

load_dotenv()

app = FastAPI()

PROD = os.getenv('PROD', '0') == '1'

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


# TODO: probably need measurement link
class LabelTask(Base):
    __tablename__ = 'label_tasks'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    fmc_id: Mapped[str] = mapped_column(String, index=True)
    measurement_checksum: Mapped[str] = mapped_column(String, unique=True, index=True)
    fmc_data: Mapped[str] = mapped_column(String)
    sia_meas_id_path: Mapped[str] = mapped_column(String)

    # 0 means not sent out yet
    sent_label_request_at_epoch: Mapped[int] = mapped_column(Integer, default=0)
    last_labeler: Mapped[str | None] = mapped_column(String, nullable=True, default=None)

    is_labeled: Mapped[bool] = mapped_column(Boolean, default=False)
    label_bolf_path: Mapped[str | None] = mapped_column(String, nullable=True, default=None)

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


class Metric(Base):
    __tablename__ = 'metrics_history'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    created_at_epoch: Mapped[int] = mapped_column(Integer)

    total_labelable: Mapped[int] = mapped_column(Integer)
    labeled: Mapped[int] = mapped_column(Integer)
    not_labeled: Mapped[int] = mapped_column(Integer)
    opened: Mapped[int] = mapped_column(Integer)
    opened_pending: Mapped[int] = mapped_column(Integer)


class LabelTaskCreate(BaseModel):
    fmc_id: str
    fmc_data: str
    measurement_checksum: str
    sia_meas_id_path: str


class LabeledTask(BaseModel):
    measurement_checksum: str
    label_bolf_path: str


@app.get('/api/get_task')
async def get_task(labeler_name: str, db: Session = Depends(get_db)):
    current_time_epoch = int(time.time())
    # TODO: i need to sync every min 4 days
    # TODO: remove LabeledTask.created_at_epoch < 1739449765 filter after video is there
    # TODO: replace created_at_epoch with recorded_epoch
    db_task = db.query(LabelTask).filter(LabelTask.is_labeled == False).filter((current_time_epoch - 60 * 60 * 24 * 4) > LabelTask.sent_label_request_at_epoch).order_by(func.random()).first()
    if not db_task:
        return {'finished': True}

    db_task.last_labeler = labeler_name
    db_task.sent_label_request_at_epoch = current_time_epoch

    encoded_name = quote(labeler_name)

    sia_url = f'https://qa.sia.bosch-automotive-mlops.com/?time=99999&minimalLabelMode=true&minimalLabelModeUsername={encoded_name}&measId={db_task.sia_meas_id_path}'

    db.commit()
    db.refresh(db_task)

    return {'task': db_task, 'sia_url': sia_url}


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



@app.post('/api/set_labeled')
async def set_labeled(tasks: list[LabeledTask], db: Session = Depends(get_db)):
    if not tasks:
        raise HTTPException(status_code=400, detail='No tasks provided')

    created_labeled_tasks = []

    for task in tasks:
        db_task = db.query(LabelTask).filter(LabelTask.measurement_checksum == task.measurement_checksum).first()
        if not db_task:
            print(f'Unknown task {task}')
            continue

        db_task.label_bolf_path = task.label_bolf_path
        db_task.is_labeled = True
        db.commit()

        created_labeled_tasks.append(task.model_dump())

    return created_labeled_tasks


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

        db_task = LabelTask(
            fmc_id=task.fmc_id,
            fmc_data=task.fmc_data,
            measurement_checksum=task.measurement_checksum,
            sia_meas_id_path=task.sia_meas_id_path,
            created_at_epoch=current_time
        )
        db.add(db_task)
        db.commit()
        created_tasks.append(task.model_dump())

    return {'created_tasks': created_tasks}

@app.post('/api/skip_task')
async def set_skipped(skipped_task: SkippedTaskCreate, db: Session = Depends(get_db)):
    if not skipped_task.sia_link:
        raise HTTPException(status_code=400, detail=f'Invalid sia_link {skipped_task.sia_link}')
    skip_task = SkippedTask(sia_link=skipped_task.sia_link, skip_reason=skipped_task.skip_reason)
    db.add(skip_task)
    db.commit()
    db.refresh(skip_task)
    return skip_task


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
    total_count = db.query(LabelTask).count()
    labeled = db.query(LabelTask).filter(LabelTask.is_labeled == True).count()
    not_labeled = db.query(LabelTask).filter(LabelTask.is_labeled == False).count()
    opened = db.query(LabelTask).filter(LabelTask.sent_label_request_at_epoch != 0).count()

    metrics = {
        'total_labelable': total_count,
        'labeled': labeled,
        'not_labeled': not_labeled,
        'opened': opened,
        'opened_pending': opened - labeled
    }

    metric = Metric(**metrics, created_at_epoch=int(time.time()))
    db.add(metric)
    db.commit()

    return metrics


@app.get('/api/metrics_history')
async def get_metrics_history(db: Session = Depends(get_db)):
    return db.query(Metric).order_by(Metric.created_at_epoch).all()


@app.get('/api/leaderboard')
async def leaderboard(db: Session = Depends(get_db)):
    leaderboard_data = db.query(LabelTask.last_labeler, func.count()).filter(LabelTask.last_labeler != None).group_by(LabelTask.last_labeler).all()
    leaderboard = {}
    for labeler, count in leaderboard_data:
        leaderboard[labeler] = count
    
    sorted_leaderboard_items = sorted(leaderboard.items(), key=lambda item: item[1], reverse=True)
    top_20_items = sorted_leaderboard_items[:20]
    return {labeler: count for labeler, count in top_20_items}

@app.get('/api/db_cleanup')
async def db_cleanup(valid_ids: list[str], db: Session = Depends(get_db)):
    rows_deleted = db.query(LabelTask).filter(
        ~LabelTask.fmc_id.in_(valid_ids)
    ).delete(synchronize_session=False)
    db.commit()
    print(f'Deleted', rows_deleted)

    remaining_rows = db.query(LabelTask).all()
    print("Remaining rows:")
    for row in remaining_rows:
        if row.fmc_id not in valid_ids:
            print(f'Problem, {row.fmc_id} not valid but still in table')


@app.get('/')
async def get_index_html(response: Response):
    response.headers['Cache-Control'] = 'no-cache'
    return FileResponse('/static/index.html')

@app.get('/leaderboard')
async def get_leaderboard_html(response: Response):
    response.headers['Cache-Control'] = 'no-cache'
    # TODO: add html text saying this not live, updated every day or so
    return FileResponse('/static/leaderboard.html')

@app.get('/map')
async def get_map(labeler_name: str, level: int):
    level_to_map_id = max(0, min(level // 4, 6))
    return FileResponse(f'/static/img/stage{level_to_map_id}.png')

app.mount('/examples', StaticFiles(directory='/static/img/examples/'), name='examples')

Base.metadata.create_all(engine)

# TODO: probably not needed
if __name__ == '__main__':
    uvicorn.run('main:app', host='0.0.0.0', port=7100, reload=True)
