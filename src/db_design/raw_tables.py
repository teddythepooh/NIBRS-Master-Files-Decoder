from sqlalchemy import MetaData, Integer, String, Date
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

metadata_obj = MetaData(schema = "raw")   


'''
administrative_segment:
  segment_level: [0, 2]
  state_code: [2, 4]
  ori: [4, 13]
  incident_number: [13, 25]
  incident_date: [25, 33]
  report_date_indicator: [33, 34]
  incident_date_hour: [34, 36]
  total_offense_segments: [36, 38]
  total_victim_segments: [38, 41]
  total_offender_segments: [41, 43]
  total_arrestee_segments: [43, 45]
  city_submission: [45, 49]
  cleared_exceptionally: [49, 50]
  exceptional_clearance_date: [50, 58]
'''

class Base(DeclarativeBase):
    meta_data = metadata_obj

class Administrative(Base):
    __tablename__ = "administrative_segment"
    
    segment_level: Mapped[str] = mapped_column(String(2))
    state_code: Mapped[str] = mapped_column(String(2))
    ori: Mapped[str] = mapped_column(String(9))
    incident_number: Mapped[str] = mapped_column(String(12))
    incident_date: Mapped[int] = mapped_column(Date)
