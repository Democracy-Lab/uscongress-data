import re
# This script runs a pre-check on candidate speaker lines to repair speakers that are split by hyphens and leaves the rest unchanged

# -------------------------------------------------------------------
# Regex to detect speaker lines
# -------------------------------------------------------------------

SPEAKER_REGEX = re.compile(
    r'^\s{0,2}'
    r'(?:'
      r'(?:M(?:r|rs|s)[.,:;]|Miss|Chairman|Chairwoman|HON[.,:;]|Hon[.,:;]|Dr[.,:;])\s?(?:Counsel\s?)?'

      r'(?:[A-Z]\.\s?)*'
      r'[A-Z][a-z]{0,3}[A-Za-z\'’]+'
      r'(?:\s[A-Z]\.)*'
      r'(?:-[A-Za-z\'’]+)*'
      r'(?:\s[A-Z][a-z]{0,3}[A-Za-z\'’]+(?:-[A-Za-z\'’]+)*)*'
      r'(?:\s(?:of\s[A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)*))?'
      r'(?:\s\[continuing\])?'
      r'(?:\s?\([^)]*\))?[.]'
    r'|'
      r'(?:'
        r'[TF]he\s(?:'
          r'CLERK'
          r'|Acting\s?CHAIR'
          r'|ACTING\s?CHAIR'
          r'|CHAIR'
          r'|CHAIRMAN(?:\s?pro\s?tempore)?'
          r'|PRESIDING\s?OFFICER'
          r'|SPEA[KR]ER(?:\s?pro\s?tempore)?'
          r'|SP\.\s?pro\s?tempore'
          r'|[VW]ICE\s?PRESIDENT'
          r'|[VW]ICE-PRESIDENT'
          r'|PRESIDENT\s?pro\s?tempore'
          r'|PRESID\.\s?pro\s?tempore'
          r'|ACTING\s?PRESIDENT\s?pro\s?tempore'
          r'|CHIEF\s?JUSTICE'
          r'|OFFICER'
        r')'
        r'(?:\s?\([^)]*\))?'
        r'\.'  # accepts only period to end speaker name -- commas occur bc of OCR errors but allowing commas brings in other issues
      r')'
    r')',
    re.MULTILINE
)

# -------------------------------------------------------------------
# NEW: Cheap "candidate speaker line" checker
# Must cover ALL possible beginnings from SPEAKER_REGEX.
# -------------------------------------------------------------------
CHEAP_SPEAKER_START_REGEX = re.compile(
    r'^\s{1,2}(?:'
      r'M(?:r|rs|s)[.,]'
      r'|Miss'
      r'|Chairman'
      r'|Chairwoman'
      r'|HON\.'
      r'|Hon'
      r'|Dr\.'
      r'|The\s(?:'
          r'CLERK'
          r'|Acting\sCHAIR'
          r'|ACTING\sCHAIR'
          r'|CHAIR'
          r'|CHAIRMAN(?:\spro\stempore)?'
          r'|PRESIDING\sOFFICER'
          r'|SPEAKER(?:\spro\stempore)?'
          r'|VICE\sPRESIDENT'
          r'|VICE-PRESIDENT'
          r'|PRESIDENT\spro\stempore'
          r'|ACTING\sPRESIDENT\spro\stempore'
          r'|CHIEF\sJUSTICE'
          r'|OFFICER'
      r')'
    r')\b'
)

# -------------------------------------------------------------------
# Regex to catch lines of underscores OR dashes as end-of-speech markers
# -------------------------------------------------------------------
UNDERLINE_REGEX = re.compile(
    r'^[ _\-]*_{3,}[ _\-]*$'
    r'|'
    r'^[ _\-]*-{3,}[ _\-]*$',
    re.MULTILINE
)

# -------------------------------------------------------------------
# Regex to catch the "[Congressional Record Volume ...]" header
# -------------------------------------------------------------------
DOC_HEADER_REGEX = re.compile(
    r'^\[Congressional Record Volume.*\]$',
    re.MULTILINE
)

# -------------------------------------------------------------------
# Regex to match a line ending in "DD, YYYY" with optional period
# -------------------------------------------------------------------
DATE_END_REGEX = re.compile(r'\d{1,2}, \d{4}\.?$')

# -------------------------------------------------------------------
# Regex to catch lines with >5 equal signs as NOTE boundaries
# -------------------------------------------------------------------
EQUALS_REGEX = re.compile(r'^[ \t]*={5,}.*$', re.MULTILINE)


# -------------------------------------------------------------------
# New: patterns for extraneous cleanup
# -------------------------------------------------------------------
EXTRAS_PATTERNS = [
    re.compile(r'\([^)]*asked and was given permission[^)]*\)\s*'),
    re.compile(r'[-_]{3,}'),
    re.compile(r'\{time\}\s*\d{2,4}'),
    re.compile(r'\[Roll[^\]\r\n]*\]'),
    re.compile(r'\[\[Page [A-Za-z0-9]{1,10}\]\]'),
    re.compile(r'<title>.*?</title>', re.IGNORECASE | re.DOTALL),
    re.compile(r'</?[^>]+>', re.IGNORECASE)
]

def _clean_extraneous(text: str) -> str:
    """
    Remove the extraneous bits defined above, keeping surrounding text.
    """
    for pat in EXTRAS_PATTERNS:
        text = pat.sub('', text)
    return text


def _strip_note_blocks(text):
    """
    Remove content between NOTE markers (lines with >=5 '='), but keep the markers themselves.
    """
    lines = text.splitlines(keepends=True)
    filtered = []
    skip = False
    for ln in lines:
        if EQUALS_REGEX.match(ln):
            filtered.append(ln)
            skip = not skip
            continue
        if skip:
            continue
        filtered.append(ln)
    return ''.join(filtered)


def _find_deeply_indented_titles(text, tab_width=2):
    """
    Identify title lines as end-of-speech markers when:
      1. The previous line is blank.
      2. Indent > tab_width spaces.
      3. ≥50% of words start uppercase.
    Returns a list of (start_offset, end_offset) for each such line.
    """
    markers = []
    lines = text.splitlines(keepends=True)

    # Compute offsets
    offsets = []
    pos = 0
    for ln in lines:
        offsets.append(pos)
        pos += len(ln)

    for i in range(1, len(lines)):
        # 1) Blank line above
        if lines[i-1].strip() != '':
            continue

        # 2) Indent > tab_width
        indent = 0
        for ch in lines[i]:
            if ch == ' ':
                indent += 1
            elif ch == '\t':
                indent += tab_width
            else:
                break
        if indent <= tab_width:
            continue

        # 3b) Existing uppercase check only
        stripped = lines[i].strip('\r\n')
        words = re.findall(r"\b\w[\w'-]*\b", stripped)
        if not words:
            continue
        uppercase_initial = sum(1 for w in words if w[0].isupper())
        if uppercase_initial * 2 >= len(words):
            markers.append((offsets[i], offsets[i]))

    return markers


def _find_right_justified_dates(text, indent_threshold=15):
    """
    Identify date lines when:
      - Indent >= indent_threshold spaces.
      - Ends with 'DD, YYYY' optionally with period.
    """
    markers = []
    lines = text.splitlines(keepends=True)
    offsets = []
    pos = 0
    for ln in lines:
        offsets.append(pos)
        pos += len(ln)

    for i, ln in enumerate(lines):
        indent = 0
        for ch in ln:
            if ch == ' ':
                indent += 1
            elif ch == '\t':
                indent += indent_threshold
            else:
                break
        if indent < indent_threshold:
            continue
        if DATE_END_REGEX.search(ln.strip()):
            markers.append((offsets[i], offsets[i]))
    return markers


# -------------------------------------------------------------------
# NEW: Repair pass for hyphen-split speaker labels (OCR)
# Only touches lines that pass CHEAP_SPEAKER_START_REGEX.
# -------------------------------------------------------------------
def _repair_hyphenated_speaker_lines(text: str) -> str:
    lines = text.splitlines(keepends=True)
    out = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Cheap intro filter: if it doesn't look like a speaker start, do nothing.
        if not CHEAP_SPEAKER_START_REGEX.match(line):
            out.append(line)
            i += 1
            continue

        # If the full speaker regex already matches this line, do nothing.
        if SPEAKER_REGEX.match(line):
            out.append(line)
            i += 1
            continue

        # No full match: only attempt repair if the line ends with a hyphen (before newline).
        line_body = line.rstrip('\r\n')
        line_ending = line[len(line_body):]  # preserve original newline(s)

        if not line_body.endswith('-'):
            out.append(line)
            i += 1
            continue

        # If there's no next line, can't repair.
        if i + 1 >= len(lines):
            out.append(line)
            i += 1
            continue

        next_line = lines[i + 1]
        next_body = next_line.rstrip('\r\n')
        next_ending = next_line[len(next_body):]

        # Combine: remove trailing hyphen, append next line with NO added spaces.
        combo = line_body[:-1] + next_body

        m = SPEAKER_REGEX.match(combo)
        if not m:
            # Repair failed: return to original text exactly.
            out.append(line)
            i += 1
            continue

        # Repair succeeded:
        # - Put the repaired speaker label on its own line.
        # - Put all remaining text from the combined version on the next line.
        label = combo[:m.end()]
        remainder = combo[m.end():]

        out.append(label + '\n')

        # Keep all speech content; hyphen loss is allowed; join is allowed.
        # Preserve the original newline from the SECOND line.
        out.append(remainder + (next_ending if next_ending else '\n'))

        i += 2  # consumed the next line

    return ''.join(out)


def scrape(text):
    """
    Yields (speaker_label, speech_text) pairs.
    End markers:
      - New speaker.
      - Lines of ___ or ---.
      - [Congressional Record Volume ...].
      - Deeply-indented titles.
      - Right-justified dates.
      - Lines with >=5 '=' (NOTE boundaries).
    """
    # Pre-remove NOTE content
    text = _strip_note_blocks(text)

    # Normalize spaces
    text = text.replace('\u00A0', ' ')

    # NEW: repair hyphen-split speaker labels before building the global events list
    text = _repair_hyphenated_speaker_lines(text)

    events = []
    # Speaker starts
    for m in re.finditer(SPEAKER_REGEX, text):
        events.append(('speaker', m.start(), m.end(), m))
    # Underline/dash ends
    for m in UNDERLINE_REGEX.finditer(text):
        events.append(('end', m.start(), m.end(), m))
    # Doc header ends
    for m in DOC_HEADER_REGEX.finditer(text):
        events.append(('end', m.start(), m.end(), m))
    # Deep titles ends
    for start, end in _find_deeply_indented_titles(text):
        events.append(('end', start, end, None))
    # Date ends
    for start, end in _find_right_justified_dates(text):
        events.append(('end', start, end, None))
    # NOTE marker ends
    for m in EQUALS_REGEX.finditer(text):
        events.append(('end', m.start(), m.end(), m))

    # Sort events
    events.sort(key=lambda e: e[1])

    current_speaker = None
    speech_start = None
    for etype, start, end, m in events:
        if etype == 'speaker':
            if current_speaker is not None:
                raw_speech = text[speech_start:start].strip()
                cleaned = _clean_extraneous(raw_speech).strip()
                yield current_speaker, cleaned
            speech_start = end
            # drop the trailing punctuation from the label ('.' or ',')
            current_speaker = text[m.start():m.end()].strip()[:-1]
        else:
            if current_speaker is not None:
                raw_speech = text[speech_start:start].strip()
                cleaned = _clean_extraneous(raw_speech).strip()
                yield current_speaker, cleaned
                current_speaker = None
                speech_start = None

    # Final speech
    if current_speaker is not None and speech_start is not None:
        raw_speech = text[speech_start:].strip()
        cleaned = _clean_extraneous(raw_speech).strip()
        yield current_speaker, cleaned