import re

# -------------------------------------------------------------------
# Regex to detect speaker lines
# -------------------------------------------------------------------

SPEAKER_REGEX = re.compile(
    r'^\s{1,2}'
    r'(?:'
      r'(?:M(?:r|rs|s)\.|Miss|Chairman|Chairwoman|HON\.|Dr\.)\s(?:Counsel\s)?'
      r'(?:[A-Z]\.\s)*'
      r'[A-Z][a-z]{0,3}[A-Za-z\']+'
      r'(?:\s[A-Z]\.)*'
      r'(?:-[A-Za-z\']+)*'
      r'(?:\s[A-Z][a-z]{0,3}[A-Za-z\']+(?:-[A-Za-z\']+)*)*'
      r'(?:\s(?:of\s[A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)*))?'
      r'(?:\s\[continuing\])?'
      r'(?:\s\([^)]*\))?\.'
    r'|'
      r'(?:'
        r'The\s(?:'
          r'CLERK'
          r'|Acting\sCHAIR'
          r'|ACTING\sCHAIR'
          r'|CHAIR'
          r'|CHAIRMAN(?:\spro\stempore)?'
          r'|PRESIDING\sOFFICER'
          r'|SPEAKER(?:\spro\stempore)?'
          r'|VICE\sPRESIDENT'
          r'|PRESIDENT\spro\stempore'
          r'|ACTING\sPRESIDENT\spro\stempore'
          r'|CHIEF\sJUSTICE'
        r')'
        r'(?:\s\([^)]*\))?'
        r'\.'
      r')'
    r')',
    re.MULTILINE
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
            # drop the trailing period from the label
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
