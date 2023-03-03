import ray.data
import ray
import ray.cloudpickle as pickle
from collections import defaultdict
from dataclasses import dataclass
from typing import List
import requests
from pprint import pprint
import os
from api import get_call_data, get_transcript_data, Monologue, Sentence


def get_transcript(call_id):
    call_ids = [int(call_id)]
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
    return transcript_monologues, transcript_text

def to_ms_int(segment):
    segment["start"] = round(segment["start"] * 1000)
    segment["end"] = round(segment["end"] * 1000)
    return Sentence(text=segment["text"], start_ts=segment["start"], end_ts=segment["end"])

def modify_ts(segment, delta):
    segment.start_ts -= delta
    segment.end_ts -= delta
    return segment

def align_timestamps(segments, gong_monologues):
    segments = [to_ms_int(segment) for segment in segments]
    delta_start = segments[0].start_ts - gong_monologues[0].start_ts
    return [modify_ts(segment, delta_start) for segment in segments]
    
import itertools
from copy import deepcopy
import re

def pairwise(iterable):
    "s -> (s0, s1), (s1, s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)

end_of_sentence_regex = r"[\.\!\?\-\–]$"
start_of_sentence_regex = r"^[A-Z]"

def reverse_enumerate(data: list):
    for i in range(len(data)-1, -1, -1):
        yield (i, data[i])

def merge_speakers(transcript_monologues):
    original_transcript_monologues = transcript_monologues
    transcript_monologues = deepcopy(transcript_monologues)
    for i in range(3):
        merged_monologues = []
        last_monologue = transcript_monologues[0]
        for monologue, next_monologue in pairwise(transcript_monologues):

            # If we have three monologues, like:
            # SPEAKER A, SPEAKER B, SPEAKER A
            # and the middle monologue is not a full sentence, merge them.
            if monologue.sentences and last_monologue.sentences and last_monologue.speaker == next_monologue.speaker and last_monologue.speaker != monologue.speaker:
                if not re.match(start_of_sentence_regex, monologue[0].text) and not re.search(end_of_sentence_regex, monologue[-1].text):
                    merged_monologues.pop()
                    monologue.sentences = last_monologue.sentences + monologue.sentences + next_monologue.sentences
                    next_monologue.sentences = []

            # If we have two monologues, like:
            # SPEAKER A, SPEAKER A
            # merge them.
            if monologue.speaker == next_monologue.speaker:
                monologue.sentences = monologue.sentences + next_monologue.sentences
                next_monologue.sentences = []
            if monologue.sentences:
                merged_monologues.append(monologue)
            last_monologue = monologue
        if next_monologue.sentences:
            merged_monologues.append(next_monologue)
        transcript_monologues = merged_monologues

    assert "".join(["".join([y.text for y in x]) for x in original_transcript_monologues]) == "".join(["".join([y.text for y in x]) for x in transcript_monologues])
    return transcript_monologues

def assign_gong_speaker(segments, transcript_monologues):
    whisper_monologues = []
    segments_in_monologue = []
    it = iter(transcript_monologues)
    monologue = next(it)
    next_monologue = next(it)
    for segment in segments:
        if segment.start_ts >= monologue.end_ts:
            whisper_monologues.append(
                Monologue(
                    segments_in_monologue,
                    monologue.speaker_id,
                    "None",
                    monologue.call_id
                )
            )
            segments_in_monologue = []
            monologue = next_monologue
            try:
                next_monologue = next(it)
            except StopIteration:
                pass
        segments_in_monologue.append(segment)
    if segments_in_monologue:
        whisper_monologues.append(
            Monologue(
                segments_in_monologue,
                monologue.speaker_id,
                "None",
                monologue.call_id
            )
        )
    return whisper_monologues

def fix_sentences(monologues):
    monologues = deepcopy(monologues)
    for monologue, next_monologue in pairwise(monologues):
        if not next_monologue:
            continue
        if not re.match(start_of_sentence_regex, next_monologue.sentences[0].text) or not re.search(end_of_sentence_regex, monologue.sentences[-1].text):
            delta_front = -1
            delta_back = -1
            num_words_back = -1
            num_words_front = -1
            found_capital = None
            index_front = None
            index_back = None
            for i, sentence in reverse_enumerate(monologue):
                num_words_back += 1
                if found_capital is not None and re.search(end_of_sentence_regex, sentence.text):
                    index_back = i+1
                    delta_back = found_capital - sentence.end_ts
                    break
                if re.match(start_of_sentence_regex, sentence.text):
                    found_capital = sentence.start_ts

            found_end = None

            for i, sentence in enumerate(next_monologue):
                num_words_front += 1
                if found_end is not None and re.match(start_of_sentence_regex, sentence.text):
                    index_front = i
                    delta_front = sentence.start_ts - found_end
                    break

                if re.search(end_of_sentence_regex, sentence.text):
                    found_end = sentence.end_ts

           # print(f"delta_front {delta_front} delta_back {delta_back}")
           # print(f"num_words_front {num_words_front} num_words_back {num_words_back}")
           # print(str(monologue))
           # print(str(next_monologue))

            # See where the delta is higher (assume a single person talking will have
            # shorter pauses between their sentences), unless we have just one word.

            if num_words_back == 1:
                delta_back = float("inf")

            if num_words_front == 1:
                delta_front = float("inf")

            if delta_front > delta_back and index_front is not None:
                index = index_front
                monologue.sentences = monologue.sentences + next_monologue.sentences[:index] 
                next_monologue.sentences = next_monologue.sentences[index:]
            elif index_back is not None:
                index = index_back
                next_monologue.sentences = monologue.sentences[index:] + next_monologue.sentences
                monologue.sentences = monologue.sentences[:index]
    return monologues