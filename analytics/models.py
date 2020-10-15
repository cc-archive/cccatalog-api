import enum
from sqlalchemy import Integer, Column, Enum, String, DateTime, Boolean, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Image(Base):
    __tablename__ = "image"
    # Managed by Django ORM; partially duplicated here so we can join
    # analytics and image data together. This is excluded from migrations.
    id = Column(Integer, primary_key=True)
    identifier = Column(UUID)
    source = Column(String)
    provider = Column(String)
    title = Column(String)


class EventMixin(object):
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, server_default=func.now(), index=True)


class ReportMixin(object):
    id = Column(Integer, primary_key=True)
    start_time = Column(DateTime, index=True)
    end_time = Column(DateTime, index=True)


class SearchEvent(Base, EventMixin):
    """
    Store searches linked to a session UUID.
    """
    __tablename__ = "search_event"

    query = Column(String, index=True)
    session_uuid = Column(UUID, index=True)


class SearchRatingEvent(Base, EventMixin):
    """
    Users can provide feedback about the quality of search results.
    """
    __tablename__= "search_rating_event"

    query = Column(String, index=True)
    relevant = Column(Boolean, index=True)


class ResultClickedEvent(Base, EventMixin):
    """
    Link result clicks to search sessions.
    """
    __tablename__ = "result_clicked_event"

    session_uuid = Column(UUID, index=True)
    result_uuid = Column(UUID, index=True)
    query = Column(String, index=True)
    result_rank = Column(Integer)


class DetailPageEvents(enum.Enum):
    ATTRIBUTION_CLICKED = enum.auto()
    REUSE_SURVEY = enum.auto()
    SOURCE_CLICKED = enum.auto()
    CREATOR_CLICKED = enum.auto()
    SHARED_SOCIAL = enum.auto()


class DetailPageEvent(Base, EventMixin):
    """
    Events that happen on result pages, such as clicking an attribution button
    or sharing the result on social media.
    """
    __tablename__ = "detail_page_event"

    result_uuid = Column(UUID, index=True)
    event_type = Column(Enum(DetailPageEvents), index=True)


class AttributionReferrerEvent(Base, EventMixin):
    """
    Triggered by a user's browser loading one of our static assets on a non-CC
    site. By parsing server logs, we can determine which work was embedded and
    on which domain it appeared.
    """
    __tablename__ = "attribution_referrer_event"

    image_uuid = Column(UUID, index=True)
    full_referer = Column(String)
    referer_domain = Column(String, index=True)
    # The path to the embedded asset on our server. ex: /static/img/cc-by.svg
    resource = Column(String, index=True)

# Reports


class UsageReport(Base, ReportMixin):
    """ Tracks statistics for the last 24 hours """
    __tablename__ = "usage_reports"
    results_clicked = Column(Integer)
    attribution_buttonclicks = Column(Integer)
    survey_responses = Column(Integer)
    source_clicked = Column(Integer)
    creator_clicked = Column(Integer)
    shared_social = Column(Integer)
    sessions = Column(Integer)
    searches = Column(Integer)
    attribution_referer_hits = Column(Integer)
    avg_rating = Column(Float)
    avg_searches_per_session = Column(Float)


class SourceUsageReport(Base, ReportMixin):
    __tablename__ = "source_report"

    source_id = Column(String, index=True)
    result_clicks = Column(Integer, index=True)


class AttributionRefererReport(Base, ReportMixin):
    __tablename__ = "attribution_referer_report"

    domain = Column(String, index=True)
    hits = Column(Integer, index=True)


class TopSearchesReport(Base, ReportMixin):
    __tablename__ = "top_searches"
    term = Column(String, index=True)
    hits = Column(Integer, index=True)


class TopResultsReport(Base, ReportMixin):
    __tablename__ = "top_results"
    result_uuid = Column(UUID, index=True)
    hits = Column(Integer, index=True)
    source = Column(String, index=True)
    title = Column(String, index=True)