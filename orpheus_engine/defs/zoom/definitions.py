import dagster as dg

from .users import zoom_users
from .daily_usage import zoom_daily_usage
from .meetings import (
    zoom_meetings,
    zoom_meeting_participants,
    zoom_meeting_participant_details,
    zoom_meeting_participant_qos,
    zoom_meeting_sharing,
    zoom_meeting_polls,
    zoom_meeting_qa,
)
from .recordings import (
    zoom_recordings,
    zoom_recording_analytics,
    zoom_meeting_summaries,
    zoom_cloud_recording_usage,
)


defs = dg.Definitions(
    assets=[
        zoom_users,
        zoom_daily_usage,
        zoom_meetings,
        zoom_meeting_participants,
        zoom_meeting_participant_details,
        zoom_meeting_participant_qos,
        zoom_meeting_sharing,
        zoom_meeting_polls,
        zoom_meeting_qa,
        zoom_recordings,
        zoom_recording_analytics,
        zoom_meeting_summaries,
        zoom_cloud_recording_usage,
    ],
)
