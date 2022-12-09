"""

"""
import os
from datetime import datetime, date, timedelta
import json
from typing import List, Dict
import sys

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, Date, Text, Boolean, ForeignKey, PrimaryKeyConstraint, DateTime
from sqlalchemy.ext.declarative import declarative_base

PATH_ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)
PATH_CONFIG = os.path.join(PATH_ROOT, 'Config', 'Config.json')

from helper.simpleLogger import MyLogger


# ================ #
CHECKING_N_DAYS = 2

# ================ #


Base = declarative_base()


class MostActivateTicker(Base):
    __tablename__ = 'MostActivateTicker'

    Date = Column(Date, primary_key=True)
    Product = Column(String(128), primary_key=True)
    Num = Column(Integer, primary_key=True)
    Ticker = Column(String(128))
    TotalVolume = Column(Float)
    TotalValue = Column(Float)

    __table_args__ = (
        PrimaryKeyConstraint('Date', 'Product', 'Num'),
        {},
    )

    def to_dict(self):
        out = {}
        for column in self.__table__.columns:
            out[column.name] = getattr(self, column.name, None)
        return out

    def to_string(self):
        return ','.join([str(i) for i in list(self.to_dict().values())])

    def __str__(self):
        return json.dumps(str(self.to_dict()), indent=4, ensure_ascii=False)

    def __repr__(self):
        return f"MostActivateTicker(" \
               f"Date={self.Date}, Product={self.Product}, Num={self.Num}, " \
               f"Ticker={self.Ticker}, TotalVolume={self.TotalVolume}, TotalValue={self.TotalValue})"

    def key_string(self):
        return f"{self.Date},{self.Product},{self.Num}"


class MostActivateTickerToDB:
    def __init__(
            self,
            user, pwd, host, db, logger=MyLogger('class MostActivateTickerToDB')
    ):
        engine = create_engine(
            # echo=True参数表示连接发出的 SQL 将被记录到标准输出
            # future=True是为了方便便我们充分利用sqlalchemy2.0样式用法
            f'mssql+pymssql://{user}:{pwd}@{host}/{db}',
            echo=False,
        )
        Base.metadata.create_all(engine)  # 首次创建表
        Session = sessionmaker(bind=engine)
        self.session = Session()
        #
        self.logger = logger

    def upload_new_data_from_files(self, root, checking_n_days=1, activate_num=1):
        def _check_files_to_upload(_root) -> list:
            # 检查的日期
            _checking_date_start: date = (datetime.now() - timedelta(days=checking_n_days-1)).date()
            # 查找所需要上传的 数据文件
            _l_files_path = []
            _l_date_files_name = os.listdir(_root)
            _l_date_files_name.sort()
            for _file_name in _l_date_files_name[::-1]:
                # 是否文件夹
                _p_file = os.path.join(_root, _file_name)
                if not os.path.isfile(_p_file):
                    continue
                # 是否日期
                try:
                    _s_date = _file_name.split(".")[0]
                    _dt_date = datetime.strptime(_s_date, "%Y%m%d").date()
                except :
                    continue
                # 是否目标日期
                if _dt_date >= _checking_date_start:
                    _l_files_path.append(_p_file)
                else:
                    break
            return _l_files_path

        def _gen_obj_from_file(p) -> List[MostActivateTicker]:
            # 读取文件 添加对象
            _l_data = []
            with open(p) as f:
                l_lines = f.readlines()
            for line in l_lines:
                line = line.strip()
                if line == '':
                    continue
                line_split = line.split(",")
                if line_split[3]:
                    total_value = float(line_split[3])
                else:
                    total_value = 0
                if line_split[2]:
                    total_volume = float(line_split[2])
                else:
                    total_volume = 0
                _l_data.append(MostActivateTicker(
                    Date=dt_date,
                    Product=line_split[0],
                    Ticker=line_split[1],
                    TotalVolume=total_volume,
                    TotalValue=total_value,
                    Num=activate_num,
                ))
            return _l_data

        #
        path_root = os.path.abspath(root)
        assert os.path.isdir(path_root)

        # 检查需要上传的哪些文件
        l_checking_folder_path = _check_files_to_upload(path_root)
        if not l_checking_folder_path:
            self.logger.warning('no checking date folder')
            return

        # 添加对象
        for file_path in l_checking_folder_path:
            s_date = os.path.basename(file_path).split(".")[0]
            dt_date = datetime.strptime(s_date, '%Y%m%d').date()
            self.logger.info(file_path)

            # 读取文件 添加对象
            l_new_data: List[MostActivateTicker] = _gen_obj_from_file(file_path)

            # 检查是否已经存在
            _db_rtn: List[MostActivateTicker] = self.session.scalars(
                select(MostActivateTicker).where(MostActivateTicker.Date == dt_date)).all()
            if not _db_rtn:
                # 上传
                try:
                    self.session.add_all(l_new_data)
                    self.session.commit()
                except Exception as e:
                    self.logger.error(e)
            else:
                # 删除旧数据
                l_new_data_key = [_.key_string() for _ in l_new_data]
                _is_delete = False
                for _old_data in _db_rtn:
                    if _old_data.key_string() in l_new_data_key:
                        self.session.delete(_old_data)
                        _is_delete = True
                if _is_delete:
                    self.logger.info(f'Delete old data, {s_date}')
                    self.session.commit()
                # 添加
                try:
                    self.session.add_all(l_new_data)
                    self.session.commit()
                except Exception as e:
                    self.logger.error(e)


if __name__ == '__main__':
    d_config = json.loads(open(PATH_CONFIG).read())
    obj = MostActivateTickerToDB(
        **d_config,
        logger=MyLogger('MostActivateTickerToDB', output_root=os.path.join(PATH_ROOT, 'logs'))
    )
    obj.upload_new_data_from_files(os.path.join(PATH_ROOT, 'Output_MostActTicker'), checking_n_days=CHECKING_N_DAYS, activate_num=1)
    obj.upload_new_data_from_files(os.path.join(PATH_ROOT, 'Output_SecondActTicker'), checking_n_days=CHECKING_N_DAYS, activate_num=2)
