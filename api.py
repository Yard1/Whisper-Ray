from collections import defaultdict
from dataclasses import dataclass
from typing import List
import requests
from pprint import pprint
import os
from functools import lru_cache


call_ids = [8269649578171048284]

base_url = "https://us-95680.api.gong.io/v2"

def get_auth_header():
    return f"Basic {os.environ['GONG_API_TOKEN']}"

CALLS_CACHE = {}

def _make_hashable(lst):
    return tuple(sorted(set(lst)))

#@lru_cache
def _get_call_data(call_ids):
    call_ids = list(call_ids)
    response = requests.post(
        f"{base_url}/calls/extensive",
        headers={'Authorization': get_auth_header()},
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
    ret = response.json()
    for call in ret["calls"]:
        CALLS_CACHE[int(call["metaData"]["id"])] = call
    return ret

def get_call_data(call_ids):
    calls_not_in_cache = set()
    calls = []
    for call_id in call_ids:
        if call_id not in CALLS_CACHE:
            calls_not_in_cache.add(call_id)
        else:
            calls.append(CALLS_CACHE[call_id])

    if calls_not_in_cache:
        ret = _get_call_data(_make_hashable(calls_not_in_cache))
        calls.extend(ret["calls"])
    return {"calls": calls}

#@lru_cache
def _get_transcript_data(call_ids):
    call_ids = list(call_ids)
    response = requests.post(
        f"{base_url}/calls/transcript",
        headers={'Authorization': get_auth_header()},
        json={
            "filter": {
                "callIds": call_ids,
            }
        }
    )
    return response.json()

def get_transcript_data(call_ids):
    return _get_transcript_data(_make_hashable(call_ids))

@lru_cache
def get_user(user_id):
    response = requests.get(
        f"{base_url}/users/{user_id}",
        headers={'Authorization': get_auth_header()},
    )
    return response.json()

@dataclass
class Sentence:
    text: str
    start_ts: int
    end_ts: int

    def __str__(self):
        return self.text


@dataclass
class Monologue:
    sentences: List[Sentence]
    speaker_id: str
    topic: str
    call_id: int

    def __getitem__(self, index):
        return self.sentences.__getitem__(index)

    def __len__(self):
        return len(self.sentences)

    def __bool__(self):
        return bool(self.sentences)

    @property
    def speaker(self):
        call_data = get_call_data([self.call_id])["calls"][0]
        speaker = next((p for p in call_data["parties"] if p["speakerId"] == self.speaker_id), None)
        if not speaker:
            return self.speaker_id

        return speaker["name"]

    @property
    def start_ts(self):
        if not self.sentences:
            return None
        return self.sentences[0].start_ts

    @property
    def end_ts(self):
        if not self.sentences:
            return None
        return self.sentences[-1].end_ts

    def __str__(self):
        if not self.sentences:
            return ""
        return f"({self.sentences[0].start_ts}) {self.speaker.upper()}: {' '.join([str(s) for s in self.sentences])}\n"


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
                
            mono = Monologue(sentences, speaker_id, topic, call_id=int(call_id))
            transcript_monologues.append(mono)

            monologue_str = " ".join(monologue_str)
            call_transcript.append(str(mono))
            # transcript_text += "\n".join(monologue_str)
            # transcript_text += "------\n\n"
        
        call_summary[call_id].update({
            "transcript": "\n".join(call_transcript)
        })
        transcript_text += "\n".join(call_transcript)

    print(call_summary)
    # pprint(call_summary)