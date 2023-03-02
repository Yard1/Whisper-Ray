from collections import defaultdict
from dataclasses import dataclass
from typing import List
import requests
from pprint import pprint
import os


call_ids = [3166028376916322699]

base_url = "https://us-95680.api.gong.io/v2"
auth_header = f"Basic {os.environ['GONG_API_TOKEN']}"

def get_call_data(call_ids):
    response = requests.post(
        f"{base_url}/calls/extensive",
        headers={'Authorization': auth_header},
        json={
            "contentSelector": {
                "context": "Basic",
                "exposedFields": {
                    "collaboration": {
                        "publicComments": True
                    },
                    "content": {
                        "pointsOfInterest": True,
                        "structure": True,
                        "topics": True,
                        "trackerOccurrences": False,
                        "trackers": True
                    },
                    "interaction": {
                        "personInteractionStats": True,
                        "questions": True,
                        "speakers": True,
                        "video": True
                    },
                    "media": True,
                    "parties": True
                }, 
            },

            "filter": {
                "callIds": call_ids,
            }
        }
    )
    return response.json()


def get_transcript_data(call_ids):
    response = requests.post(
        f"{base_url}/calls/transcript",
        headers={'Authorization': auth_header},
        json={
            "filter": {
                "callIds": call_ids,
            }
        }
    )
    return response.json()


@dataclass
class Sentence:
    text: str
    start_ts: int
    end_ts: int

    def __str__(self):
        return f"({self.start_ts}) {self.text}"


@dataclass
class Monologue:
    sentences: List[Sentence]
    speaker_id: str
    topic: str

    def __str__(self):
        return " ".join([str(s) for s in sentences]) + "\n"


if __name__ == "__main__":
    calls_data = get_call_data(call_ids).get("calls")


    call_summary = defaultdict(dict)
    for call_data in calls_data:
        call_id = call_data["metaData"]["id"]
        call_title = call_data["metaData"]["title"]
        media_data = call_data.get("media")
        if media_data:
            call_summary[call_id].update({
                "call_id": call_id,
                "title": call_title,
                "audio": media_data.get("audioUrl"),
                "video": media_data.get("videoUrl"),
            })

    transcripts_data = get_transcript_data(call_ids).get("callTranscripts")
    transcript_monologues = []
    transcript_text = ""
    for t_data in transcripts_data:
        call_id = t_data["callId"]
        call_transcript = []

        for mono_data in t_data["transcript"]:
            speaker_id = mono_data["speakerId"]
            topic = mono_data["topic"]
            sentences = []
            monologue_str = []
            for s in mono_data["sentences"]:
                sentences.append(
                    Sentence(s["text"], s["start"], s["end"])
                )
                monologue_str.append(s["text"])
                
            mono = Monologue(sentences, speaker_id, topic)
            transcript_monologues.append(mono)

            monologue_str = " ".join(monologue_str)
            call_transcript.append(str(mono))
            # transcript_text += "\n".join(monologue_str)
            # transcript_text += "------\n\n"
        
        call_summary[call_id].update({
            "transcript": "\n".join(call_transcript)
        })
        transcript_text += "\n".join(call_transcript)

    print(transcript_text)
    # pprint(call_summary)