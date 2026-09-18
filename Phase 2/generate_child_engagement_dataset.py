#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==========================================================================================
 child_engagement_analytics  --  SYNTHETIC DATASET GENERATOR + DATA-QUALITY PROBLEM INJECTOR
==========================================================================================

Generates the 8 tables of the provided ERD (organization, child, child_organization,
activity, preferences, lifestyle, engagement, child_preference), first as a completely
clean, relationally-valid dataset, then injects every data-quality / data-collection /
analytical problem specified in the project brief, and finally exports one CSV per table
plus a data_quality_report.csv.

ERD IS NOT MODIFIED: same tables, same columns, same keys, same relationships.

Run:
    pip install pandas numpy faker
    python generate_child_engagement_dataset.py

Pipeline:
    Imports -> Configuration -> Helpers -> Parent tables -> Dependent tables ->
    Relationships -> Validate clean -> Inject problems -> Validate final ->
    Data-quality report -> Export CSV -> main()
==========================================================================================
"""

# ==========================================================================================
# 1. IMPORTS
# ==========================================================================================

import os
import sys
import time
import random
import string
import warnings
from datetime import date, datetime

import numpy as np
import pandas as pd
from faker import Faker

warnings.filterwarnings("ignore")


# ==========================================================================================
# 2. CONFIGURATION  (everything tunable lives here)
# ==========================================================================================

# ---------------------------------------------------------------- reproducibility
SEED = 42

# ---------------------------------------------------------------- output
OUTPUT_DIR = "output"
CSV_FLOAT_FORMAT = "%.2f"

# ---------------------------------------------------------------- row counts
N_ORGANIZATIONS = 60          # parent table
N_CHILDREN = 12_000           # parent table  (>10k)
N_ACTIVITIES = 1_500          # depends on organization
N_PREFERENCES = 40            # small lookup table (logically small)
N_ENGAGEMENT = 150_000        # depends on child + activity (>>10k)

# child_organization rows  ~= N_CHILDREN * mean(orgs per child)   (>10k)
MIN_ORGS_PER_CHILD = 1
MAX_ORGS_PER_CHILD = 3
P_MULTI_ORG = 0.42            # probability a child is enrolled in more than one org

# child_preference rows    ~= N_CHILDREN * mean(prefs per child)  (>10k)
MIN_PREFS_PER_CHILD = 1
MAX_PREFS_PER_CHILD = 6

# lifestyle rows == N_CHILDREN (1:1 with child)

# ---------------------------------------------------------------- calendar window
ENROLLMENT_START = date(2024, 1, 1)
ENROLLMENT_END = date(2025, 9, 30)
SESSION_END = date(2026, 6, 30)

# ---------------------------------------------------------------- name pools (Faker)
FIRST_NAME_POOL = 2_500
LAST_NAME_POOL = 1_500

# ---------------------------------------------------------------- ERROR RATES / INJECTION PARAMETERS
# GLOBAL DIAL: multiplies every probability-style rate below.
#   0.5 = lightly dirty      1.0 = dirty (default)      2.0 = heavily corrupted
MESSINESS_LEVEL = 1.0
MAX_RATE_AFTER_SCALING = 0.90     # no single rate may exceed this after scaling

# Every injected data-quality problem is controlled from this dictionary.
ERROR_RATES = {
    # --- P01 referential / structural integrity -----------------------------------------
    "p01_deleted_children_frac":        0.03,   # children hard-deleted from child (rows stay elsewhere)
    "p01_deleted_activities_frac":      0.035,   # activities hard-deleted from activity
    "p01_engagement_bad_child_frac":    0.015,   # engagement.child_id -> id that never existed
    "p01_engagement_bad_activity_frac": 0.015,   # engagement.activity_id -> id that never existed
    "p01_orphan_child_org_frac":        0.02,   # child_organization.child_id -> non-existent child
    "p01_activity_bad_org_frac":        0.03,   # activity.org_id -> non-existent organization

    # --- P02 impossible / contradictory values ------------------------------------------
    "p02_age_out_of_range_frac":        0.025,   # bypasses CHECK (age between 3 and 18)
    "p02_score_out_of_range_frac":      0.02,   # engagement_score outside 0..100
    "p02_completion_out_of_range_frac": 0.018,   # task_completion_rate outside 0..100
    "p02_negative_duration_frac":       0.04,   # activity.duration < 0
    "p02_negative_response_time_frac":  0.015,   # engagement.response_time < 0
    "p02_negative_devices_frac":        0.015,   # child.number_of_devices < 0
    "p02_negative_shifts_frac":         0.012,   # engagement.number_of_attention_shifts < 0

    # --- P03 unit / scale inconsistency (minutes logged instead of hours) ----------------
    "p03_orgs_logging_minutes_frac":    0.22,    # share of orgs whose pipeline logged minutes
    "p03_child_affected_frac":          0.85,    # share of those orgs' children actually affected
    "p03_partial_columns_prob":         0.35,    # sometimes only SOME columns are in minutes

    # --- P04 duplicate / near-duplicate children ----------------------------------------
    "p04_duplicate_children_frac":      0.045,   # share of children that get a near-duplicate twin
    "p04_exact_duplicate_share":        0.25,    # of those, share that are exact-name duplicates
    "p04_sessions_per_duplicate":       6,       # engagement rows created for each twin

    # --- P05 missing-data mechanisms (MCAR / MAR / MNAR) --------------------------------
    "p05_income_mnar_base":             0.05,    # baseline non-response on household_income
    "p05_income_mnar_low_income":       0.45,    # low-income households skip the income question
    "p05_income_mnar_high_income":      0.18,    # very high income also skips (privacy)
    "p05_working_hours_mcar":           0.12,    # parents_working_hours missing completely at random
    "p05_devices_mar_public":           0.14,    # number_of_devices missingness depends on school_type
    "p05_devices_mar_private":          0.03,
    "p05_devices_mar_homeschool":       0.20,
    "p05_devices_mar_international":    0.05,
    "p05_lifestyle_mcar":               0.06,
    "p05_engagement_metric_mcar":       0.045,   # observer/sensor drop-outs per metric
    "p05_activity_meta_missing":        0.09,    # incomplete activity catalogue entries    # sporadic nulls in lifestyle diary columns

    # --- P06 timestamp / session logic --------------------------------------------------
    "p06_session_before_enrollment_frac": 0.035, # session_date earlier than enrollment_date
    "p06_overlapping_sessions_frac":      0.03, # same child, same day, duplicated/overlapping rows
    "p06_future_session_frac":            0.008, # session_date in the future
    "p06_null_session_date_frac":         0.015,

    # --- P07 inconsistent categorical encoding ------------------------------------------
    "p07_activity_cat_frac":            0.45,    # share of activity rows with dirty categorical text
    "p07_child_cat_frac":               0.4,    # share of child rows with dirty categorical text
    "p07_whitespace_frac":              0.15,    # extra leading/trailing whitespace
    "p07_null_like_frac":               0.03,   # "N/A", "unknown", "" style tokens

    # --- P08 self-report / proxy-report bias --------------------------------------------
    "p08_teen_underreport_frac":        0.55,    # teens (13-18) underreporting social media/gaming
    "p08_teen_underreport_strength":    0.45,    # up to 45% shrink of the true value
    "p08_parent_underreport_frac":      0.50,    # parents of young kids underestimate screen_time
    "p08_parent_underreport_strength":  0.30,
    "p08_rounding_heaping_frac":        0.45,    # heaping on whole / half hours (recall bias)

    # --- P09 instrument / observer measurement drift ------------------------------------
    "p09_drifting_orgs_frac":           0.35,    # share of orgs with a deviant rating rubric
    "p09_offset_range":                 (-12.0, 12.0),   # additive rubric offset on engagement_score
    "p09_compression_range":            (0.55, 1.35),    # multiplicative scale (range restriction)
    "p09_attention_unit_orgs_frac":     0.10,    # orgs that logged avg_attention_duration in minutes

    # --- P10 confounded difficulty ratings ----------------------------------------------
    "p10_relabeling_orgs_frac":         0.25,    # orgs that systematically inflate/deflate difficulty
    "p10_relabel_frac_within_org":      0.60,

    # --- P11 temporal / seasonal confounding --------------------------------------------
    "p11_summer_engagement_boost":      6.0,     # additive seasonal effect (Jun-Aug)
    "p11_winter_engagement_penalty":    -5.0,    # additive seasonal effect (Dec-Feb)
    "p11_weekend_boost":                3.5,
    "p11_summer_outdoor_multiplier":    1.45,    # seasonal effect on lifestyle.outdoor_time

    # --- P12 survivorship bias (drop-outs) ----------------------------------------------
    "p12_dropout_pairs_frac":           0.22,    # share of (child, org) enrolments that end early
    "p12_pre_dropout_decline":          9.0,     # engagement decline just before dropping out

    # --- P13 outlier-driven distortion --------------------------------------------------
    "p13_outlier_orgs":                 5,       # number of orgs polluted by extreme outliers
    "p13_outliers_per_org":             80,      # extreme (very young + very high score) rows
    "p13_extreme_attention_frac":       0.004,  # absurd avg_attention_duration values

    # --- P14 non-representative device access / non-response bias -----------------------
    "p14_low_income_tech_nonresponse":  0.28,    # low-income rows drop access/devices answers
    "p14_tech_overreport_frac":         0.10,    # socially-desirable over-reporting of devices

    # --- P15 dirty lookup tables (organization / preferences) ---------------------------
    "p15_duplicate_orgs_frac":          0.12,    # same org registered twice under a variant name
    "p15_org_type_dirty_frac":          0.35,    # casing / abbreviation noise in organization.type
    "p15_org_type_missing_frac":        0.10,
    "p15_org_name_dirty_frac":          0.25,    # whitespace / casing noise in organization.name
    "p15_duplicate_prefs_frac":         0.15,    # duplicated preference options
    "p15_pref_category_dirty_frac":     0.30,
    "p15_pref_category_missing_frac":   0.08,

    # --- P16 bridge-table problems ------------------------------------------------------
    "p16_null_enrollment_date_frac":    0.05,
    "p16_sentinel_enrollment_frac":     0.02,    # 1900-01-01 / 1970-01-01 sentinels
    "p16_future_enrollment_frac":       0.015,
    "p16_bad_preference_id_frac":       0.010,   # child_preference.preference_id -> nowhere
    "p16_enrollment_after_sessions_frac": 0.02,  # enrolment recorded after the child's sessions

    # --- P17 bulk-load duplicate ROWS (double ingestion of a CSV batch) -----------------
    "p17_dup_child_rows_frac":          0.010,   # full duplicate rows, same child_id
    "p17_dup_lifestyle_rows_frac":      0.012,
    "p17_dup_engagement_rows_frac":     0.008,   # same session_id ingested twice
    "p17_dup_child_org_rows_frac":      0.015,   # breaks the composite primary key
    "p17_dup_child_pref_rows_frac":     0.015,

    # --- P18 free-text / format chaos ---------------------------------------------------
    "p18_mixed_session_date_frac":      0.28,    # DD/MM/YYYY, textual months, impossible dates
    "p18_mixed_enrollment_date_frac":   0.22,
    "p18_income_as_text_frac":          0.09,    # "$12,500", "12 500 EGP", "approx 9000"
    "p18_name_dirty_frac":              0.08,    # CASING / padding / "Unknown" / empty names
    "p18_name_missing_frac":            0.025,
    "p18_numeric_as_text_frac":         0.04,    # numeric columns polluted with text tokens
}


def apply_messiness_level(level=MESSINESS_LEVEL):
    """Scale every probability-style rate by the global messiness dial."""
    if level == 1.0:
        return ERROR_RATES
    for k, v in list(ERROR_RATES.items()):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            if 0.0 < v <= 1.0:                       # a probability / fraction
                ERROR_RATES[k] = min(v * level, MAX_RATE_AFTER_SCALING)
    return ERROR_RATES

# ---------------------------------------------------------------- analytical structure knobs
# Simpson's paradox: WITHIN every school_type more screen time lowers engagement, but the
# school types with the most screen time also have the highest baseline engagement, so the
# naive POOLED correlation comes out positive. Both halves are configurable.
SIMPSON_SCREEN_SHIFT_BY_SCHOOL = {       # extra daily screen hours per school type
    "Public": 0.0, "Private": 1.6, "Homeschool": -1.5, "International": 3.2,
}
SIMPSON_ENGAGEMENT_OFFSET_BY_SCHOOL = {  # baseline engagement offset per school type
    "Public": 0.0, "Private": 12.0, "Homeschool": -10.0, "International": 25.0,
}
WITHIN_GROUP_SCREEN_EFFECT = -2.00       # within-group slope (negative on purpose)

# ---------------------------------------------------------------- domain vocabularies
ORG_TYPES = ["School", "Camp", "Clinic", "Community Center", "After-School Program", "NGO"]
ORG_TYPE_WEIGHTS = [0.34, 0.14, 0.13, 0.19, 0.15, 0.05]

INTERACTION_TYPES = ["Individual", "Pair", "Group"]
ACTIVITY_FORMATS = ["Digital", "Physical", "Hybrid"]
PARTICIPATION_TYPES = ["Voluntary", "Assigned"]
DIFFICULTY_LEVELS = ["Easy", "Medium", "Hard"]
ENVIRONMENT_TYPES = ["Urban", "Suburban", "Rural"]
SCHOOL_TYPES = ["Public", "Private", "Homeschool", "International"]

ACTIVITY_TOPICS = [
    "Storytelling", "Robotics Lab", "Puzzle Challenge", "Music Circle", "Drawing Studio",
    "Coding Basics", "Team Sports", "Science Experiment", "Math Games", "Drama Workshop",
    "Reading Club", "Chess Tactics", "Gardening", "Lego Build", "Memory Training",
    "Dance Session", "Creative Writing", "Nature Walk", "Board Games", "Origami",
    "Cooking Basics", "Mindfulness", "Debate Club", "Photography", "Animation Studio",
]
ACTIVITY_SUFFIXES = ["Session", "Workshop", "Club", "Lab", "Program", "Track", "Module"]

ORG_NAME_PREFIX = [
    "Green", "Bright", "Nile", "Cedar", "Harbor", "Sunrise", "Maple", "Pioneer", "Lotus",
    "Falcon", "Crescent", "Summit", "Delta", "Horizon", "Aurora", "Silver", "Oakwood",
    "Riverside", "Northgate", "Blue Lake",
]
ORG_NAME_CORE = ["Children", "Youth", "Kids", "Junior", "Learning", "Growth", "Discovery"]

# Preference catalogue: category -> candidate names
PREFERENCE_CATALOGUE = {
    "interaction_type": ["Individual", "Pair", "Group"],
    "activity_format": ["Digital", "Physical", "Hybrid"],
    "difficulty_level": ["Easy", "Medium", "Hard"],
    "topic": [
        "Storytelling", "Robotics Lab", "Puzzle Challenge", "Music Circle", "Drawing Studio",
        "Coding Basics", "Team Sports", "Science Experiment", "Math Games", "Drama Workshop",
        "Reading Club", "Chess Tactics", "Gardening", "Lego Build", "Memory Training",
        "Dance Session", "Creative Writing", "Nature Walk", "Board Games", "Origami",
        "Cooking Basics", "Mindfulness", "Debate Club", "Photography", "Animation Studio",
    ],
    "movement": ["High Movement", "Low Movement"],
    "session_length": ["Short Sessions", "Long Sessions"],
}

# Dirty-encoding variants used by P07 (inconsistent categorical encoding)
CATEGORY_VARIANTS = {
    "Individual": ["individual", "INDIVIDUAL", "Indiv", "IND", "individual "],
    "Pair": ["pair", "PAIR", "Pairs", "2-person", "PR"],
    "Group": ["group", "GROUP", "GRP", "Grp", "group work"],
    "Digital": ["digital", "DIGITAL", "Online", "online", "DIG"],
    "Physical": ["physical", "PHYSICAL", "Offline", "In-person", "PHY"],
    "Hybrid": ["hybrid", "HYBRID", "Blended", "mixed", "HYB"],
    "Voluntary": ["voluntary", "VOLUNTARY", "Opt-in", "opt in", "VOL"],
    "Assigned": ["assigned", "ASSIGNED", "Mandatory", "mandatory", "ASG"],
    "Easy": ["easy", "EASY", "Low", "level 1", "E"],
    "Medium": ["medium", "MEDIUM", "Med", "Moderate", "level 2"],
    "Hard": ["hard", "HARD", "High", "Difficult", "level 3"],
    "Urban": ["urban", "URBAN", "City", "city", "URB"],
    "Suburban": ["suburban", "SUBURBAN", "Sub-urban", "Suburb", "SUB"],
    "Rural": ["rural", "RURAL", "Village", "countryside", "RUR"],
    "Public": ["public", "PUBLIC", "Gov", "Governmental", "PUB"],
    "Private": ["private", "PRIVATE", "Priv", "PVT", "private "],
    "Homeschool": ["homeschool", "HOMESCHOOL", "Home School", "home-school", "HS"],
    "International": ["international", "INTERNATIONAL", "Intl", "INTL", "Int."],
}
NULL_LIKE_TOKENS = ["N/A", "n/a", "NA", "unknown", "Unknown", "-", "", "null", "NULL", "?"]

# ---------------------------------------------------------------- global state
fake = Faker()
rng = np.random.default_rng(SEED)

# Every injector appends a record here -> used by the data-quality report.
INJECTION_LOG = []


# ==========================================================================================
# 3. HELPER FUNCTIONS
# ==========================================================================================

def set_seeds(seed=SEED):
    """Make the whole pipeline reproducible."""
    global rng, fake
    random.seed(seed)
    np.random.seed(seed % (2 ** 32 - 1))
    rng = np.random.default_rng(seed)
    Faker.seed(seed)
    fake = Faker()
    return rng


def log_injection(problem_id, problem_name, tables, n_records, details=""):
    """Record one injected data-quality problem for the final report."""
    INJECTION_LOG.append({
        "problem_id": problem_id,
        "problem_name": problem_name,
        "tables_affected": ", ".join(tables) if isinstance(tables, (list, tuple)) else str(tables),
        "records_affected": int(n_records),
        "details": details,
    })


def banner(text, char="=", width=90):
    print("\n" + char * width)
    print(text)
    print(char * width)


def pick_indices(n_rows, fraction, generator=None):
    """Pick a random subset of row positions (0..n_rows-1) of the given fraction."""
    g = generator if generator is not None else rng
    if n_rows <= 0 or fraction <= 0:
        return np.array([], dtype=np.int64)
    k = int(round(n_rows * fraction))
    k = max(0, min(k, n_rows))
    if k == 0:
        return np.array([], dtype=np.int64)
    return g.choice(n_rows, size=k, replace=False)


def date_to_ordinal(d):
    return d.toordinal()


def ordinals_to_dates(arr):
    """Vectorised int-ordinal -> pandas datetime64 conversion (NaN-safe)."""
    arr = np.asarray(arr, dtype="float64")
    out = np.full(arr.shape, np.datetime64("NaT"), dtype="datetime64[D]")
    mask = ~np.isnan(arr)
    base = np.datetime64("0001-01-01", "D")
    out[mask] = base + (arr[mask].astype("int64") - 1)
    return pd.to_datetime(out)


def truncated_normal(mean, sd, low, high, size, generator=None):
    """Fast rejection-free truncated normal (clip-based, then jitter to avoid mass at bounds)."""
    g = generator if generator is not None else rng
    vals = g.normal(mean, sd, size)
    vals = np.clip(vals, low, high)
    return vals


def name_pools():
    """Pre-generate name pools once (Faker is slow per-call)."""
    firsts = list({fake.first_name() for _ in range(int(FIRST_NAME_POOL * 1.4))})
    lasts = list({fake.last_name() for _ in range(int(LAST_NAME_POOL * 1.6))})
    while len(firsts) < 200:
        firsts.append(fake.first_name())
    while len(lasts) < 200:
        lasts.append(fake.last_name())
    return np.array(firsts, dtype=object), np.array(lasts, dtype=object)


def misspell(word, generator=None):
    """Produce a realistic near-duplicate spelling of a name."""
    g = generator if generator is not None else rng
    if not isinstance(word, str) or len(word) < 3:
        return word
    mode = g.integers(0, 7)
    i = int(g.integers(1, len(word) - 1))
    if mode == 0:                                    # drop a letter
        return word[:i] + word[i + 1:]
    if mode == 1:                                    # double a letter
        return word[:i] + word[i] + word[i:]
    if mode == 2:                                    # swap two letters
        return word[:i] + word[i + 1:i + 2] + word[i:i + 1] + word[i + 2:]
    if mode == 3:                                    # common substitutions
        table = {"y": "i", "i": "y", "c": "k", "k": "c", "s": "z", "z": "s",
                 "ph": "f", "e": "a", "a": "e", "o": "u", "u": "o"}
        for src, dst in table.items():
            if src in word.lower():
                low = word.lower()
                j = low.find(src)
                return word[:j] + dst + word[j + len(src):]
        return word + "e"
    if mode == 4:                                    # case noise
        return word.upper() if g.random() < 0.5 else word.lower()
    if mode == 5:                                    # stray whitespace / hyphen
        return (" " + word) if g.random() < 0.5 else (word[:i] + "-" + word[i:])
    return word + g.choice(list(string.ascii_lowercase))


def dirty_category(value, generator=None):
    """Return an inconsistently-encoded version of a clean categorical value."""
    g = generator if generator is not None else rng
    variants = CATEGORY_VARIANTS.get(value)
    if not variants:
        return value
    return str(variants[int(g.integers(0, len(variants)))])


def to_datetime_safe(series):
    """Parse a date column that may contain several formats and garbage tokens."""
    try:
        return pd.to_datetime(series, errors="coerce", format="mixed", dayfirst=False)
    except Exception:
        try:
            return pd.to_datetime(series.astype(str), errors="coerce", format="mixed")
        except Exception:
            return pd.to_datetime(series, errors="coerce")


def safe_int_series(series):
    """Convert to pandas nullable Int64 while tolerating NaN / floats."""
    return pd.to_numeric(series, errors="coerce").round().astype("Float64").astype("Int64")


def month_of(ordinals):
    """Vectorised month extraction from ordinal ints (NaN-safe)."""
    dts = ordinals_to_dates(ordinals)
    return pd.Series(dts).dt.month.to_numpy()


def weekday_of(ordinals):
    dts = ordinals_to_dates(ordinals)
    return pd.Series(dts).dt.weekday.to_numpy()


# ==========================================================================================
# 4. GENERATE PARENT TABLES
# ==========================================================================================

def generate_organization(n=N_ORGANIZATIONS):
    """
    Parent table. Each org also carries hidden simulation traits (quality, rubric bias,
    recruitment channel) used by later generators. Hidden traits are NOT exported.
    """
    org_ids = np.arange(1, n + 1, dtype=np.int64)
    types = rng.choice(ORG_TYPES, size=n, p=ORG_TYPE_WEIGHTS)

    names = []
    used = set()
    for i in range(n):
        while True:
            nm = (f"{ORG_NAME_PREFIX[int(rng.integers(0, len(ORG_NAME_PREFIX)))]} "
                  f"{ORG_NAME_CORE[int(rng.integers(0, len(ORG_NAME_CORE)))]} "
                  f"{types[i]} {i + 1:03d}")
            if nm not in used:
                used.add(nm)
                names.append(nm)
                break

    org = pd.DataFrame({
        "org_id": org_ids,
        "name": names,
        "type": types,
    })

    # ---- hidden simulation traits (selection bias / programme quality / observer rubric)
    quality = rng.normal(0.0, 4.5, n)                       # programme quality effect
    # Clinics recruit children already flagged for attention concerns -> lower baseline
    channel_bias = np.select(
        [types == "Clinic", types == "School", types == "Camp",
         types == "Community Center", types == "After-School Program", types == "NGO"],
        [-8.5, 0.5, 3.0, -1.0, 1.0, -2.0],
        default=0.0,
    )
    income_bias = np.select(
        [types == "Private" , types == "Clinic", types == "NGO", types == "Camp"],
        [0.0, -0.10, -0.35, 0.30],
        default=0.0,
    )
    org_hidden = pd.DataFrame({
        "org_id": org_ids,
        "org_quality": quality,
        "org_channel_bias": channel_bias,
        "org_income_bias": income_bias,
        "org_rubric_offset": np.zeros(n),          # filled by P09
        "org_rubric_scale": np.ones(n),            # filled by P09
        "org_logs_minutes": np.zeros(n, dtype=bool),   # filled by P03
        "org_attention_minutes": np.zeros(n, dtype=bool),
    })
    return org, org_hidden


def generate_child(n=N_CHILDREN, org_hidden=None):
    """
    Parent table. Socio-demographics generated with realistic dependencies:
    environment_type -> income -> school_type -> tech access -> devices.
    """
    child_ids = np.arange(1, n + 1, dtype=np.int64)

    firsts, lasts = name_pools()
    fname = rng.choice(firsts, size=n)
    lname = rng.choice(lasts, size=n)

    # Age: bimodal-ish, most children 6-14
    age = np.rint(truncated_normal(10.5, 3.6, 3, 18, n)).astype(np.int64)

    # Environment
    environment_type = rng.choice(ENVIRONMENT_TYPES, size=n, p=[0.48, 0.34, 0.18])

    # Household income (log-normal) depends on environment
    env_mu = np.select(
        [environment_type == "Urban", environment_type == "Suburban", environment_type == "Rural"],
        [10.30, 10.15, 9.75], default=10.0,
    )
    household_income = np.exp(rng.normal(env_mu, 0.55, n))
    household_income = np.clip(household_income, 1_500, 480_000)

    # School type depends on income + environment (MAR mechanism source)
    inc_z = (np.log(household_income) - np.log(household_income).mean()) / np.log(household_income).std()
    p_private = 1 / (1 + np.exp(-(-1.55 + 1.35 * inc_z)))
    p_intl = 1 / (1 + np.exp(-(-3.10 + 1.15 * inc_z)))
    p_home = np.where(environment_type == "Rural", 0.11, 0.05)
    p_public = np.clip(1 - p_private - p_intl - p_home, 0.02, None)
    probs = np.vstack([p_public, p_private, p_home, p_intl])
    probs = probs / probs.sum(axis=0, keepdims=True)
    u = rng.random(n)
    cum = np.cumsum(probs, axis=0)
    idx = (u > cum).sum(axis=0)
    school_type = np.array(SCHOOL_TYPES, dtype=object)[np.clip(idx, 0, 3)]

    # Family structure
    family_size = np.clip(rng.poisson(np.where(environment_type == "Rural", 5.4, 4.2), n), 2, 12)
    num_of_siblings = np.clip(family_size - 2, 0, None)
    num_of_siblings = np.where(rng.random(n) < 0.10, np.clip(num_of_siblings - 1, 0, None), num_of_siblings)

    # Parents' working hours per week
    parents_working_hours = np.round(
        truncated_normal(42 + 3.2 * inc_z, 11.0, 0, 90, n), 1
    )

    # Tech access & devices (strongly income-driven -> confounder for engagement)
    p_tech = 1 / (1 + np.exp(-(0.95 + 1.45 * inc_z)))
    access_to_technology = (rng.random(n) < p_tech).astype(np.int64)
    lam = np.clip(0.9 + 1.25 * np.exp(inc_z * 0.55), 0.3, 9.0)
    number_of_devices = rng.poisson(lam, n)
    number_of_devices = np.where(access_to_technology == 1, np.clip(number_of_devices, 1, 14), 0)

    child = pd.DataFrame({
        "child_id": child_ids,
        "fname": fname,
        "lname": lname,
        "age": age,
        "family_size": family_size.astype(np.int64),
        "num_of_siblings": num_of_siblings.astype(np.int64),
        "parents_working_hours": parents_working_hours,
        "household_income": np.round(household_income, 2),
        "access_to_technology": access_to_technology,
        "number_of_devices": number_of_devices.astype(np.int64),
        "environment_type": environment_type,
        "school_type": school_type,
    })

    # ---- hidden per-child traits used downstream
    child_hidden = pd.DataFrame({
        "child_id": child_ids,
        "inc_z": inc_z,
        "ability": rng.normal(0, 1, n),                 # latent engagement propensity
        "attention_trait": rng.normal(0, 1, n),         # latent attention stability
        "activity_rate": np.clip(rng.gamma(3.0, 0.4, n), 0.15, 4.0),  # sessions propensity
        "true_income": np.round(household_income, 2),   # kept before MNAR deletion
    })
    return child, child_hidden


def generate_preferences(n=N_PREFERENCES):
    """Parent lookup table of preference options."""
    rows = []
    for category, names in PREFERENCE_CATALOGUE.items():
        for nm in names:
            rows.append({"name": nm, "category": category})
    df = pd.DataFrame(rows)
    if len(df) > n:
        df = df.sample(n=n, random_state=SEED).reset_index(drop=True)
        # guarantee the structural categories survive sampling
        must = pd.DataFrame(
            [{"name": v, "category": c}
             for c in ("interaction_type", "activity_format", "difficulty_level")
             for v in PREFERENCE_CATALOGUE[c]]
        )
        df = pd.concat([must, df]).drop_duplicates(subset=["name", "category"]).reset_index(drop=True)
    df.insert(0, "preference_id", np.arange(1, len(df) + 1, dtype=np.int64))
    return df


# ==========================================================================================
# 5. GENERATE DEPENDENT TABLES / RELATIONSHIPS
# ==========================================================================================

def generate_activity(organization, org_hidden, n=N_ACTIVITIES):
    """Each activity belongs to exactly one organization (1:N)."""
    org_ids = organization["org_id"].to_numpy()
    n_org = len(org_ids)
    n = max(n, n_org * 3)

    # guarantee >=3 activities per org, distribute the rest by org size weight
    base = np.repeat(org_ids, 3)
    remaining = n - len(base)
    weights = rng.gamma(3.0, 1.0, n_org)
    weights = weights / weights.sum()
    extra = rng.choice(org_ids, size=max(remaining, 0), p=weights)
    all_org = np.sort(np.concatenate([base, extra]))          # sorted -> contiguous per org

    n = len(all_org)
    activity_ids = np.arange(1, n + 1, dtype=np.int64)

    otypes = organization.set_index("org_id")["type"].to_dict()
    org_type_arr = np.array([otypes[o] for o in all_org], dtype=object)

    topic = rng.choice(ACTIVITY_TOPICS, size=n)
    suffix = rng.choice(ACTIVITY_SUFFIXES, size=n)
    names = np.array([f"{t} {s}" for t, s in zip(topic, suffix)], dtype=object)

    # Activity format correlates with topic + org type
    digital_topics = {"Coding Basics", "Robotics Lab", "Animation Studio", "Photography", "Math Games"}
    physical_topics = {"Team Sports", "Dance Session", "Nature Walk", "Gardening", "Cooking Basics"}
    p_digital = np.where(np.isin(topic, list(digital_topics)), 0.62, 0.20)
    p_physical = np.where(np.isin(topic, list(physical_topics)), 0.65, 0.34)
    p_hybrid = np.clip(1 - p_digital - p_physical, 0.05, None)
    pmat = np.vstack([p_digital, p_physical, p_hybrid])
    pmat = pmat / pmat.sum(axis=0, keepdims=True)
    u = rng.random(n)
    fmt_idx = (u > np.cumsum(pmat, axis=0)).sum(axis=0)
    activity_format = np.array(ACTIVITY_FORMATS, dtype=object)[np.clip(fmt_idx, 0, 2)]

    interaction_type = rng.choice(INTERACTION_TYPES, size=n, p=[0.30, 0.22, 0.48])
    group_size = np.where(
        interaction_type == "Individual", 1,
        np.where(interaction_type == "Pair", 2, np.clip(rng.poisson(9, n), 3, 30)),
    ).astype(np.int64)

    participation_type = np.where(
        np.isin(org_type_arr, ["School", "Clinic"]),
        rng.choice(PARTICIPATION_TYPES, size=n, p=[0.35, 0.65]),
        rng.choice(PARTICIPATION_TYPES, size=n, p=[0.78, 0.22]),
    )

    difficulty_level = rng.choice(DIFFICULTY_LEVELS, size=n, p=[0.36, 0.44, 0.20])

    # Duration (minutes): harder/group activities run longer
    base_dur = 30 + 10 * (difficulty_level == "Medium") + 22 * (difficulty_level == "Hard")
    base_dur = base_dur + 8 * (interaction_type == "Group")
    duration = np.rint(np.clip(rng.normal(base_dur, 12, n), 10, 150)).astype(np.int64)

    movement_required = np.where(
        activity_format == "Physical", (rng.random(n) < 0.93),
        np.where(activity_format == "Hybrid", (rng.random(n) < 0.55), (rng.random(n) < 0.12)),
    ).astype(np.int64)

    activity = pd.DataFrame({
        "activity_id": activity_ids,
        "org_id": all_org.astype(np.int64),
        "name": names,
        "duration": duration,
        "group_size": group_size,
        "interaction_type": interaction_type,
        "activity_format": activity_format,
        "participation_type": participation_type,
        "difficulty_level": difficulty_level,
        "movement_required": movement_required,
    })

    activity_hidden = pd.DataFrame({
        "activity_id": activity_ids,
        "topic": topic,
        "design_quality": rng.normal(0, 3.0, n),
        "true_difficulty": np.select(
            [difficulty_level == "Easy", difficulty_level == "Medium", difficulty_level == "Hard"],
            [1, 2, 3], default=2,
        ).astype(np.int64),
    })
    return activity, activity_hidden


def generate_child_organization(child, organization, org_hidden):
    """M:N bridge. Enrolment date + hidden dropout date (used by the survivorship problem)."""
    n_children = len(child)
    child_ids = child["child_id"].to_numpy()
    org_ids = organization["org_id"].to_numpy()

    n_orgs_per_child = np.where(
        rng.random(n_children) < P_MULTI_ORG,
        rng.integers(MIN_ORGS_PER_CHILD + 1, MAX_ORGS_PER_CHILD + 1, n_children),
        MIN_ORGS_PER_CHILD,
    )

    rep_child = np.repeat(child_ids, n_orgs_per_child)
    total_pairs = len(rep_child)

    # org popularity weights (bigger orgs enrol more children)
    pop = rng.gamma(4.0, 1.0, len(org_ids))
    pop = pop / pop.sum()
    picked_org = rng.choice(org_ids, size=total_pairs, p=pop)

    pairs = pd.DataFrame({"child_id": rep_child, "org_id": picked_org})
    pairs = pairs.drop_duplicates(subset=["child_id", "org_id"]).reset_index(drop=True)

    n_pairs = len(pairs)
    start_ord = date_to_ordinal(ENROLLMENT_START)
    end_ord = date_to_ordinal(ENROLLMENT_END)
    enroll_ord = start_ord + np.rint(
        rng.beta(2.0, 2.2, n_pairs) * (end_ord - start_ord)
    ).astype(np.int64)

    pairs["enrollment_ord"] = enroll_ord
    pairs["enrollment_date"] = ordinals_to_dates(enroll_ord)

    # hidden dropout date for a share of enrolments (used by P12)
    session_end_ord = date_to_ordinal(SESSION_END)
    span = np.clip(session_end_ord - enroll_ord, 30, None)
    dropout_ord = enroll_ord + np.rint(rng.uniform(0.25, 0.95, n_pairs) * span).astype(np.int64)
    is_dropout = rng.random(n_pairs) < ERROR_RATES["p12_dropout_pairs_frac"]
    pairs["dropout_ord"] = np.where(is_dropout, dropout_ord, session_end_ord + 5_000)

    child_organization = pairs[["child_id", "org_id", "enrollment_date"]].copy()
    co_hidden = pairs[["child_id", "org_id", "enrollment_ord", "dropout_ord"]].copy()
    return child_organization, co_hidden


def generate_child_preference(child, preferences):
    """M:N bridge. Preferences correlate with age and school type (not uniform noise)."""
    pref_ids = preferences["preference_id"].to_numpy()
    pref_names = preferences["name"].to_numpy()
    pref_cats = preferences["category"].to_numpy()

    n_children = len(child)
    ages = child["age"].to_numpy()
    child_ids = child["child_id"].to_numpy()

    # --- one structural preference per category of interest, biased by age
    def pick_weighted(options, weights_matrix):
        cum = np.cumsum(weights_matrix / weights_matrix.sum(axis=0, keepdims=True), axis=0)
        u = rng.random(n_children)
        idx = (u > cum).sum(axis=0)
        return np.array(options, dtype=object)[np.clip(idx, 0, len(options) - 1)]

    w_inter = np.vstack([
        0.25 + 0.030 * (ages - 3),                 # Individual grows with age
        np.full(n_children, 0.25),                 # Pair
        0.80 - 0.030 * (ages - 3),                 # Group shrinks with age
    ])
    pref_interaction = pick_weighted(INTERACTION_TYPES, np.clip(w_inter, 0.05, None))

    w_fmt = np.vstack([
        0.15 + 0.045 * (ages - 3),                 # Digital grows with age
        0.85 - 0.040 * (ages - 3),                 # Physical shrinks
        np.full(n_children, 0.35),                 # Hybrid
    ])
    pref_format = pick_weighted(ACTIVITY_FORMATS, np.clip(w_fmt, 0.05, None))

    name_cat_to_id = {(n, c): i for n, c, i in zip(pref_names, pref_cats, pref_ids)}

    rows_child, rows_pref = [], []
    inter_ids = np.array([name_cat_to_id.get((v, "interaction_type"), -1) for v in pref_interaction])
    fmt_ids = np.array([name_cat_to_id.get((v, "activity_format"), -1) for v in pref_format])
    ok = (inter_ids > 0)
    rows_child.append(child_ids[ok]); rows_pref.append(inter_ids[ok])
    ok = (fmt_ids > 0)
    rows_child.append(child_ids[ok]); rows_pref.append(fmt_ids[ok])

    # --- extra free preferences (topics, movement, session length, difficulty)
    other_ids = pref_ids[~np.isin(pref_cats, ["interaction_type", "activity_format"])]
    if len(other_ids) == 0:
        other_ids = pref_ids
    n_extra = rng.integers(MIN_PREFS_PER_CHILD, MAX_PREFS_PER_CHILD, n_children)
    rep_child = np.repeat(child_ids, n_extra)
    rep_pref = rng.choice(other_ids, size=len(rep_child))
    rows_child.append(rep_child); rows_pref.append(rep_pref)

    cp = pd.DataFrame({
        "child_id": np.concatenate(rows_child),
        "preference_id": np.concatenate(rows_pref),
    }).drop_duplicates().reset_index(drop=True)

    child_pref_hidden = pd.DataFrame({
        "child_id": child_ids,
        "pref_interaction": pref_interaction,
        "pref_format": pref_format,
    })
    return cp, child_pref_hidden


def generate_lifestyle(child, child_hidden):
    """
    1:1 with child. A realistic 24h budget: sleep + school/study + screen split + outdoor + play.
    Screen time is split into gaming / social media / other screen use.
    """
    n = len(child)
    ages = child["age"].to_numpy()
    inc_z = child_hidden["inc_z"].to_numpy()
    devices = child["number_of_devices"].to_numpy()
    school_type = child["school_type"].to_numpy()
    env = child["environment_type"].to_numpy()
    pwh = child["parents_working_hours"].to_numpy()
    sibs = child["num_of_siblings"].to_numpy()

    # Sleep decreases with age
    sleep_duration = np.round(np.clip(rng.normal(11.6 - 0.185 * ages, 0.85, n), 4.0, 14.0), 1)

    # Study time: rises with age, higher for private/international, lower for homeschool play-based
    study_base = 0.7 + 0.22 * (ages - 3)
    study_base = study_base + np.select(
        [school_type == "Private", school_type == "International", school_type == "Homeschool"],
        [0.8, 1.0, -0.6], default=0.0,
    )
    study_time = np.round(np.clip(rng.normal(study_base, 0.9, n), 0.0, 9.0), 1)

    # Screen time: rises with age and devices; gaming skews male-ish/younger-teen, SM skews teen
    school_screen_shift = np.array(
        [SIMPSON_SCREEN_SHIFT_BY_SCHOOL.get(s, 0.0) for s in school_type], dtype="float64")
    screen_base = 1.1 + 0.20 * (ages - 3) + 0.22 * devices + 0.25 * inc_z + school_screen_shift
    screen_time = np.round(np.clip(rng.normal(screen_base, 1.2, n), 0.0, 12.0), 1)

    # gaming / social media are SEPARATE (disjoint) blocks of screen use, so the seven
    # lifestyle columns can be summed into a daily budget.
    gaming = np.round(np.clip(
        rng.normal(np.where(ages < 13, 0.55, 0.85) + 0.05 * devices, 0.55, n), 0.0, 8.0), 1)
    social_media_usage = np.round(np.clip(
        rng.normal(np.where(ages >= 13, 1.35, 0.20) + 0.04 * devices, 0.55, n), 0.0, 8.0), 1)

    # Outdoor time: more in rural, less with high screen time, less with long parent work hours
    outdoor_base = 2.5 - 0.16 * screen_time + np.where(env == "Rural", 0.9, 0.0) \
                   - 0.012 * (pwh - 40) + 0.10 * sibs
    outdoor_time = np.round(np.clip(rng.normal(outdoor_base, 0.8, n), 0.0, 8.0), 1)

    # Free play: decreases with age, increases with siblings
    free_base = 4.2 - 0.20 * (ages - 3) + 0.18 * sibs - 0.10 * study_time
    free_play = np.round(np.clip(rng.normal(free_base, 0.9, n), 0.0, 9.0), 1)

    # ---- enforce the 24h daily budget in the CLEAN dataset
    # (violations of this budget are introduced later, on purpose, by the unit-scale bug)
    awake_cols = [screen_time, gaming, social_media_usage, outdoor_time, study_time, free_play]
    awake_total = sum(awake_cols)
    budget = np.clip(23.5 - sleep_duration, 2.0, None)
    scale = np.where(awake_total > budget, budget / np.maximum(awake_total, 0.1), 1.0)
    screen_time = np.round(screen_time * scale, 1)
    gaming = np.round(gaming * scale, 1)
    social_media_usage = np.round(social_media_usage * scale, 1)
    outdoor_time = np.round(outdoor_time * scale, 1)
    study_time = np.round(study_time * scale, 1)
    free_play = np.round(free_play * scale, 1)

    # final safety pass against rounding drift
    total = (sleep_duration + screen_time + gaming + social_media_usage
             + outdoor_time + study_time + free_play)
    fix = total > 24.0
    if fix.any():
        shave = np.where(fix, total - 23.5, 0.0)
        free_play = np.round(np.clip(free_play - shave, 0.0, None), 1)
        total = (sleep_duration + screen_time + gaming + social_media_usage
                 + outdoor_time + study_time + free_play)
        fix = total > 24.0
        if fix.any():
            shave = np.where(fix, total - 23.5, 0.0)
            screen_time = np.round(np.clip(screen_time - shave, 0.0, None), 1)

    lifestyle = pd.DataFrame({
        "child_id": child["child_id"].to_numpy(),
        "sleep_duration": sleep_duration,
        "screen_time": screen_time,
        "gaming": gaming,
        "social_media_usage": social_media_usage,
        "outdoor_time": outdoor_time,
        "study_time": study_time,
        "free_play": free_play,
    })

    lifestyle_hidden = pd.DataFrame({
        "child_id": child["child_id"].to_numpy(),
        "true_screen_time": screen_time.copy(),
        "true_gaming": gaming.copy(),
        "true_social_media": social_media_usage.copy(),
    })
    return lifestyle, lifestyle_hidden


def generate_engagement(child, child_hidden, child_pref_hidden, lifestyle, activity,
                        activity_hidden, organization, org_hidden, co_hidden,
                        n=N_ENGAGEMENT):
    """
    Dependent fact table.
    Every row is a real (child, activity) pair where the child is enrolled in the
    organization that provides the activity, and session_date >= enrollment_date.
    """
    # ---------------- 1. sample (child, org) enrolments weighted by child activity rate
    co = co_hidden.reset_index(drop=True)
    rate_map = child_hidden.set_index("child_id")["activity_rate"]
    pair_w = co["child_id"].map(rate_map).to_numpy(dtype="float64")
    pair_w = pair_w / pair_w.sum()
    pair_idx = rng.choice(len(co), size=n, p=pair_w)

    child_id = co["child_id"].to_numpy()[pair_idx]
    org_id = co["org_id"].to_numpy()[pair_idx]
    enroll_ord = co["enrollment_ord"].to_numpy()[pair_idx]

    # ---------------- 2. pick an activity inside that org (contiguous blocks, vectorised)
    act_sorted = activity.sort_values("activity_id").reset_index(drop=True)
    act_org = act_sorted["org_id"].to_numpy()
    order = np.argsort(act_org, kind="stable")
    act_org_sorted = act_org[order]
    act_ids_sorted = act_sorted["activity_id"].to_numpy()[order]

    max_org = int(act_org_sorted.max())
    starts = np.searchsorted(act_org_sorted, np.arange(0, max_org + 2), side="left")
    counts = np.diff(starts)

    org_start = starts[org_id]
    org_count = np.maximum(counts[org_id], 1)
    offset = (rng.random(n) * org_count).astype(np.int64)
    act_pos = np.clip(org_start + offset, 0, len(act_ids_sorted) - 1)
    activity_id = act_ids_sorted[act_pos]

    # ---------------- 3. session dates within the enrolment window
    session_end_ord = date_to_ordinal(SESSION_END)
    span = np.clip(session_end_ord - enroll_ord, 7, None)
    session_ord = enroll_ord + np.rint(rng.beta(1.6, 1.9, n) * span).astype(np.int64)
    session_ord = np.minimum(session_ord, session_end_ord)

    # ---------------- 4. gather features
    ch = child.set_index("child_id")
    ages = ch["age"].reindex(child_id).to_numpy()
    school_type = ch["school_type"].reindex(child_id).to_numpy()
    env_type = ch["environment_type"].reindex(child_id).to_numpy()
    devices = ch["number_of_devices"].reindex(child_id).to_numpy(dtype="float64")

    chh = child_hidden.set_index("child_id")
    ability = chh["ability"].reindex(child_id).to_numpy()
    attention_trait = chh["attention_trait"].reindex(child_id).to_numpy()
    inc_z = chh["inc_z"].reindex(child_id).to_numpy()

    ls = lifestyle.set_index("child_id")
    screen_time = ls["screen_time"].reindex(child_id).to_numpy(dtype="float64")
    sleep = ls["sleep_duration"].reindex(child_id).to_numpy(dtype="float64")

    cph = child_pref_hidden.set_index("child_id")
    pref_inter = cph["pref_interaction"].reindex(child_id).to_numpy()
    pref_fmt = cph["pref_format"].reindex(child_id).to_numpy()

    act = activity.set_index("activity_id")
    a_duration = act["duration"].reindex(activity_id).to_numpy(dtype="float64")
    a_inter = act["interaction_type"].reindex(activity_id).to_numpy()
    a_fmt = act["activity_format"].reindex(activity_id).to_numpy()
    a_move = act["movement_required"].reindex(activity_id).to_numpy(dtype="float64")

    ah = activity_hidden.set_index("activity_id")
    a_quality = ah["design_quality"].reindex(activity_id).to_numpy()
    a_diff = ah["true_difficulty"].reindex(activity_id).to_numpy(dtype="float64")

    oh = org_hidden.set_index("org_id")
    o_quality = oh["org_quality"].reindex(org_id).to_numpy()
    o_channel = oh["org_channel_bias"].reindex(org_id).to_numpy()

    # ---------------- 5. structural engagement model
    # Simpson's paradox: within every school_type screen_time hurts engagement, but the
    # school types with the highest screen time also have the highest baseline -> the
    # naive pooled correlation flips sign.
    school_offset = np.array(
        [SIMPSON_ENGAGEMENT_OFFSET_BY_SCHOOL.get(s, 0.0) for s in school_type], dtype="float64")

    age_effect = -0.16 * (ages - 11.5) ** 2 + 2.2          # inverted-U in age
    # inverted-U in duration, optimum shifts with age (dosage effect)
    optimal_duration = 28 + 2.1 * (ages - 3)
    duration_effect = -0.0040 * (a_duration - optimal_duration) ** 2 + 4.0

    fit_inter = (pref_inter == a_inter).astype(float) * 6.5
    fit_fmt = (pref_fmt == a_fmt).astype(float) * 4.5

    # difficulty x age interaction
    diff_effect = -1.9 * np.abs(a_diff - (1.0 + (ages - 3) / 7.5))

    tech_effect = 0.55 * np.clip(devices, 0, 10)           # mediator of income
    income_effect = 1.35 * inc_z                           # confounder
    sleep_effect = 1.25 * (sleep - 9.0)
    screen_effect = WITHIN_GROUP_SCREEN_EFFECT * screen_time   # within-group negative
    move_effect = np.where(env_type == "Rural", 1.4, 0.4) * a_move

    noise = rng.normal(0, 6.5, n)

    engagement_score = (
        58.0 + 5.5 * ability + age_effect + duration_effect + fit_inter + fit_fmt
        + diff_effect + tech_effect + income_effect + sleep_effect + screen_effect
        + move_effect + school_offset + o_quality + o_channel + a_quality + noise
    )
    engagement_score = np.clip(engagement_score, 0, 100)

    # ---------------- 6. attention metrics derived from the same latent process
    attention_base = 2.2 + 0.85 * (ages - 3) + 0.22 * (engagement_score - 55) / 5.0
    avg_attention_duration = np.clip(
        rng.normal(attention_base * 60 + 25 * attention_trait, 45, n), 5, 3_600
    )  # seconds

    response_time = np.clip(
        rng.gamma(3.0, 1.0, n) * (2.6 - 0.05 * (ages - 3)) * (1.5 - engagement_score / 150.0),
        0.2, 45.0,
    )  # seconds

    shifts_lambda = np.clip(
        14.0 - 0.45 * (ages - 3) - 0.09 * (engagement_score - 55) - 1.1 * attention_trait
        + 0.05 * a_duration, 0.5, 60,
    )
    number_of_attention_shifts = rng.poisson(shifts_lambda).astype(np.int64)

    task_completion_rate = np.clip(
        0.75 * engagement_score + 14
        + 0.012 * (avg_attention_duration / 6.0)
        - 0.55 * number_of_attention_shifts
        - 1.2 * (a_diff - 2)
        + rng.normal(0, 7.0, n),
        0, 100,
    )

    engagement = pd.DataFrame({
        "session_id": np.arange(1, n + 1, dtype=np.int64),
        "child_id": child_id.astype(np.int64),
        "activity_id": activity_id.astype(np.int64),
        "session_date": ordinals_to_dates(session_ord),
        "avg_attention_duration": np.round(avg_attention_duration, 2),
        "response_time": np.round(response_time, 2),
        "task_completion_rate": np.round(task_completion_rate, 2),
        "engagement_score": np.round(engagement_score, 2),
        "number_of_attention_shifts": number_of_attention_shifts,
        # internal helper columns (prefixed with "_" and dropped before export)
        "_session_ord": session_ord.astype(np.int64),
        "_org_id": org_id.astype(np.int64),
        "_pair_idx": pair_idx.astype(np.int64),
    })
    return engagement


# ==========================================================================================
# 6. VALIDATION (clean dataset + final dataset)
# ==========================================================================================

TABLE_ORDER = [
    "organization", "child", "preferences", "activity",
    "child_organization", "child_preference", "lifestyle", "engagement",
]

EXPORT_COLUMNS = {
    "organization": ["org_id", "name", "type"],
    "child": ["child_id", "fname", "lname", "age", "family_size", "num_of_siblings",
              "parents_working_hours", "household_income", "access_to_technology",
              "number_of_devices", "environment_type", "school_type"],
    "child_organization": ["child_id", "org_id", "enrollment_date"],
    "activity": ["activity_id", "org_id", "name", "duration", "group_size",
                 "interaction_type", "activity_format", "participation_type",
                 "difficulty_level", "movement_required"],
    "preferences": ["preference_id", "name", "category"],
    "lifestyle": ["child_id", "sleep_duration", "screen_time", "gaming",
                  "social_media_usage", "outdoor_time", "study_time", "free_play"],
    "engagement": ["session_id", "child_id", "activity_id", "session_date",
                   "avg_attention_duration", "response_time", "task_completion_rate",
                   "engagement_score", "number_of_attention_shifts"],
    "child_preference": ["child_id", "preference_id"],
}

PRIMARY_KEYS = {
    "organization": ["org_id"],
    "child": ["child_id"],
    "child_organization": ["child_id", "org_id"],
    "activity": ["activity_id"],
    "preferences": ["preference_id"],
    "lifestyle": ["child_id"],
    "engagement": ["session_id"],
    "child_preference": ["child_id", "preference_id"],
}

FOREIGN_KEYS = [
    # (child table, child column, parent table, parent column)
    ("child_organization", "child_id", "child", "child_id"),
    ("child_organization", "org_id", "organization", "org_id"),
    ("activity", "org_id", "organization", "org_id"),
    ("lifestyle", "child_id", "child", "child_id"),
    ("engagement", "child_id", "child", "child_id"),
    ("engagement", "activity_id", "activity", "activity_id"),
    ("child_preference", "child_id", "child", "child_id"),
    ("child_preference", "preference_id", "preferences", "preference_id"),
]


def validate_primary_keys(tables):
    issues = []
    for t, keys in PRIMARY_KEYS.items():
        df = tables[t]
        dup = int(df.duplicated(subset=keys).sum())
        nulls = int(df[keys].isna().any(axis=1).sum())
        if dup:
            issues.append(f"[PK] {t}: {dup} duplicated key rows on {keys}")
        if nulls:
            issues.append(f"[PK] {t}: {nulls} NULL key values on {keys}")
    return issues


def validate_foreign_keys(tables):
    issues = []
    for ct, ccol, pt, pcol in FOREIGN_KEYS:
        child_vals = tables[ct][ccol]
        parent_vals = set(tables[pt][pcol].dropna().unique().tolist())
        bad = int((~child_vals.isin(parent_vals) & child_vals.notna()).sum())
        if bad:
            issues.append(f"[FK] {ct}.{ccol} -> {pt}.{pcol}: {bad} violating rows")
    return issues


def count_fk_violations(tables, table_name):
    total = 0
    for ct, ccol, pt, pcol in FOREIGN_KEYS:
        if ct != table_name:
            continue
        vals = tables[ct][ccol]
        parent_vals = set(tables[pt][pcol].dropna().unique().tolist())
        total += int((~vals.isin(parent_vals) & vals.notna()).sum())
    return total


def validate_relationships(tables):
    """Cardinality / relationship rules that go beyond plain FK existence."""
    issues = []

    # lifestyle is strictly 1:1 with child
    n_child = tables["child"]["child_id"].nunique()
    n_life = tables["lifestyle"]["child_id"].nunique()
    if n_life != n_child:
        issues.append(f"[REL] lifestyle is not 1:1 with child ({n_life} vs {n_child})")

    # every engagement row must correspond to an existing enrolment in the activity's org
    eng = tables["engagement"]
    act = tables["activity"][["activity_id", "org_id"]]
    merged = eng.merge(act, on="activity_id", how="left")
    co_keys = set(map(tuple, tables["child_organization"][["child_id", "org_id"]].to_numpy()))
    pairs = list(zip(merged["child_id"].to_numpy(), merged["org_id"].to_numpy()))
    bad = sum(1 for p in pairs if p not in co_keys)
    if bad:
        issues.append(f"[REL] engagement: {bad} sessions whose child is not enrolled in the activity's org")

    # every activity belongs to exactly one org (structurally guaranteed, verified anyway)
    if tables["activity"]["activity_id"].duplicated().any():
        issues.append("[REL] activity: duplicated activity_id -> more than one org per activity")

    return issues


def validate_required_fields(tables):
    required = {
        "organization": ["org_id", "name"],
        "child": ["child_id"],
        "child_organization": ["child_id", "org_id"],
        "activity": ["activity_id", "org_id", "name"],
        "preferences": ["preference_id", "name"],
        "lifestyle": ["child_id"],
        "engagement": ["session_id", "child_id", "activity_id"],
        "child_preference": ["child_id", "preference_id"],
    }
    issues = []
    for t, cols in required.items():
        df = tables[t]
        for c in cols:
            n = int(df[c].isna().sum())
            if n:
                issues.append(f"[NOT NULL] {t}.{c}: {n} NULL values")
    return issues


def validate_business_rules(tables):
    """The CHECK constraints from the DDL + domain rules."""
    issues = []
    ch = tables["child"]
    eng = tables["engagement"]
    act = tables["activity"]
    ls = tables["lifestyle"]

    age = pd.to_numeric(ch["age"], errors="coerce")
    bad_age = int(((age < 3) | (age > 18)).sum())
    if bad_age:
        issues.append(f"[CHECK] child.age outside 3..18: {bad_age} rows")

    for col in ("task_completion_rate", "engagement_score"):
        v = pd.to_numeric(eng[col], errors="coerce")
        bad = int(((v < 0) | (v > 100)).sum())
        if bad:
            issues.append(f"[CHECK] engagement.{col} outside 0..100: {bad} rows")

    for tname, df, col in (("activity", act, "duration"),
                           ("engagement", eng, "response_time"),
                           ("engagement", eng, "avg_attention_duration"),
                           ("engagement", eng, "number_of_attention_shifts"),
                           ("child", ch, "number_of_devices"),
                           ("child", ch, "parents_working_hours")):
        v = pd.to_numeric(df[col], errors="coerce")
        bad = int((v < 0).sum())
        if bad:
            issues.append(f"[DOMAIN] {tname}.{col} negative: {bad} rows")

    time_cols = ["sleep_duration", "screen_time", "gaming", "social_media_usage",
                 "outdoor_time", "study_time", "free_play"]
    total = ls[time_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1, min_count=1)
    bad_budget = int((total > 24).sum())
    if bad_budget:
        issues.append(f"[DOMAIN] lifestyle: {bad_budget} children whose daily hours exceed 24")

    # session_date before enrollment_date
    if "session_date" in eng.columns:
        co = tables["child_organization"].copy()
        a = act[["activity_id", "org_id"]]
        m = eng[["child_id", "activity_id", "session_date"]].merge(a, on="activity_id", how="left")
        m = m.merge(co, on=["child_id", "org_id"], how="left")
        bad_dates = int((to_datetime_safe(m["session_date"])
                         < to_datetime_safe(m["enrollment_date"])).sum())
        if bad_dates:
            issues.append(f"[DOMAIN] engagement: {bad_dates} sessions dated before enrollment_date")

    # categorical vocabularies
    vocab = {
        ("activity", "interaction_type"): INTERACTION_TYPES,
        ("activity", "activity_format"): ACTIVITY_FORMATS,
        ("activity", "participation_type"): PARTICIPATION_TYPES,
        ("activity", "difficulty_level"): DIFFICULTY_LEVELS,
        ("child", "environment_type"): ENVIRONMENT_TYPES,
        ("child", "school_type"): SCHOOL_TYPES,
    }
    for (tname, col), allowed in vocab.items():
        s = tables[tname][col]
        bad = int((~s.isin(allowed) & s.notna()).sum())
        if bad:
            issues.append(f"[ENCODING] {tname}.{col}: {bad} rows outside the canonical vocabulary")

    return issues


def run_validation(tables, stage="CLEAN", strict=False):
    banner(f"VALIDATION STAGE: {stage}")
    issues = []
    issues += validate_primary_keys(tables)
    issues += validate_foreign_keys(tables)
    issues += validate_relationships(tables)
    issues += validate_required_fields(tables)
    issues += validate_business_rules(tables)

    if not issues:
        print("No issues found. Dataset is clean and relationally valid.")
    else:
        for i in issues:
            print("  -", i)
    if strict and issues:
        raise AssertionError(f"Clean-stage validation failed with {len(issues)} issues.")
    return issues


def sanity_check_final(tables):
    """
    Guard against ACCIDENTAL programming errors (as opposed to the intentional problems).
    Any failure here means a bug, not a requested data-quality issue.
    """
    banner("ACCIDENTAL-ERROR SANITY CHECKS (must all pass)")
    problems = []

    for t in TABLE_ORDER:
        df = tables[t]
        expected = EXPORT_COLUMNS[t]
        missing = [c for c in expected if c not in df.columns]
        if missing:
            problems.append(f"{t}: missing expected columns {missing}")
        extra = [c for c in df.columns if c not in expected and not c.startswith("_")]
        if extra:
            problems.append(f"{t}: unexpected extra columns {extra}")
        if len(df) == 0:
            problems.append(f"{t}: table is empty")

    # Duplicated keys are allowed ONLY in the exact amount injected by P17.
    expected_dups = tables.get("_expected_duplicate_keys", {}) if isinstance(tables, dict) else {}
    expected_dups = expected_dups or {}
    for t, keys in (("engagement", ["session_id"]), ("activity", ["activity_id"]),
                    ("organization", ["org_id"]), ("preferences", ["preference_id"]),
                    ("child", ["child_id"]), ("lifestyle", ["child_id"]),
                    ("child_organization", ["child_id", "org_id"]),
                    ("child_preference", ["child_id", "preference_id"])):
        actual = int(tables[t][keys].duplicated().sum())
        allowed = int(expected_dups.get(t, 0))
        if actual != allowed:
            problems.append(f"{t}: {actual} duplicated {keys} rows but {allowed} were injected (BUG)")

    # index must be clean for CSV export
    for t in TABLE_ORDER:
        if tables[t].index.duplicated().any():
            problems.append(f"{t}: duplicated dataframe index (BUG)")

    # columns that must never be NULL even after injection
    never_null = [("engagement", "session_id"), ("child", "child_id"),
                  ("organization", "org_id"), ("activity", "activity_id"),
                  ("preferences", "preference_id"), ("lifestyle", "child_id")]
    for t, c in never_null:
        if tables[t][c].isna().any():
            problems.append(f"{t}.{c}: contains NULLs (BUG)")

    if problems:
        for p in problems:
            print("  !! ", p)
        raise AssertionError("Accidental programming errors detected: " + "; ".join(problems))
    print("All structural sanity checks passed - only the intentional problems are present.")
    return True


# ==========================================================================================
# 7. DATA-QUALITY PROBLEM INJECTORS  (one function per problem)
# ==========================================================================================

def inject_problem_01_referential_integrity(tables, state):
    """
    P01 - Referential / structural integrity gaps.
    Simulated hard deletions in parent tables plus FK values that never existed:
      * engagement.child_id / activity_id -> missing parents
      * orphaned child_organization rows
      * activity.org_id with no matching organization
    """
    r = ERROR_RATES
    child = tables["child"]
    activity = tables["activity"]
    engagement = tables["engagement"]
    co = tables["child_organization"]
    affected = 0

    # --- simulated deletion of children (rows in engagement / child_organization / lifestyle survive)
    n_del_child = int(len(child) * r["p01_deleted_children_frac"])
    if n_del_child:
        del_children = rng.choice(child["child_id"].to_numpy(), size=n_del_child, replace=False)
        tables["child"] = child[~child["child_id"].isin(del_children)].reset_index(drop=True)
        orphan_eng = int(engagement["child_id"].isin(del_children).sum())
        orphan_co = int(co["child_id"].isin(del_children).sum())
        orphan_ls = int(tables["lifestyle"]["child_id"].isin(del_children).sum())
        affected += n_del_child + orphan_eng + orphan_co + orphan_ls
        state["deleted_children"] = del_children
        log_injection("P01a", "Simulated deletion of child records (orphans left behind)",
                      ["child", "engagement", "child_organization", "lifestyle"],
                      n_del_child + orphan_eng + orphan_co + orphan_ls,
                      f"{n_del_child} children deleted; {orphan_eng} orphan engagement rows, "
                      f"{orphan_co} orphan child_organization rows, {orphan_ls} orphan lifestyle rows")

    # --- simulated deletion of activities
    n_del_act = int(len(activity) * r["p01_deleted_activities_frac"])
    if n_del_act:
        del_acts = rng.choice(activity["activity_id"].to_numpy(), size=n_del_act, replace=False)
        tables["activity"] = tables["activity"][
            ~tables["activity"]["activity_id"].isin(del_acts)].reset_index(drop=True)
        orphan_eng = int(engagement["activity_id"].isin(del_acts).sum())
        affected += n_del_act + orphan_eng
        log_injection("P01b", "Simulated deletion of activity records (orphan sessions)",
                      ["activity", "engagement"], n_del_act + orphan_eng,
                      f"{n_del_act} activities deleted; {orphan_eng} engagement rows now orphaned")

    # --- engagement FKs pointing at ids that never existed
    max_child = int(tables["child"]["child_id"].max()) if len(tables["child"]) else 1
    idx = pick_indices(len(engagement), r["p01_engagement_bad_child_frac"])
    if len(idx):
        engagement.loc[engagement.index[idx], "child_id"] = rng.integers(
            max_child + 1_000, max_child + 90_000, len(idx))
        affected += len(idx)
        log_injection("P01c", "engagement.child_id pointing to non-existent children",
                      ["engagement"], len(idx), "ids far outside the child key space")

    max_act = int(tables["activity"]["activity_id"].max()) if len(tables["activity"]) else 1
    idx = pick_indices(len(engagement), r["p01_engagement_bad_activity_frac"])
    if len(idx):
        engagement.loc[engagement.index[idx], "activity_id"] = rng.integers(
            max_act + 1_000, max_act + 50_000, len(idx))
        affected += len(idx)
        log_injection("P01d", "engagement.activity_id pointing to non-existent activities",
                      ["engagement"], len(idx), "ids far outside the activity key space")

    # --- orphaned child_organization rows
    idx = pick_indices(len(co), r["p01_orphan_child_org_frac"])
    if len(idx):
        co.loc[co.index[idx], "child_id"] = rng.integers(max_child + 100_000,
                                                         max_child + 190_000, len(idx))
        affected += len(idx)
        log_injection("P01e", "Orphaned child_organization rows (unknown child_id)",
                      ["child_organization"], len(idx), "bridge rows with no parent child")

    # --- activities with no matching organization
    max_org = int(tables["organization"]["org_id"].max())
    act = tables["activity"]
    idx = pick_indices(len(act), r["p01_activity_bad_org_frac"])
    if len(idx):
        act.loc[act.index[idx], "org_id"] = rng.integers(max_org + 50, max_org + 900, len(idx))
        affected += len(idx)
        log_injection("P01f", "activity.org_id with no matching organization",
                      ["activity"], len(idx), "provider organisation missing")

    tables["engagement"] = engagement
    tables["child_organization"] = co
    tables["activity"] = act
    return tables


def inject_problem_02_impossible_values(tables, state):
    """
    P02 - Impossible / contradictory values that bypass the DDL CHECK constraints
    (simulating a bulk load that skipped validation).
    """
    r = ERROR_RATES
    child = tables["child"]
    eng = tables["engagement"]
    act = tables["activity"]
    total = 0

    # age outside 3..18
    idx = pick_indices(len(child), r["p02_age_out_of_range_frac"])
    if len(idx):
        weird = rng.choice([-5, 0, 1, 2, 19, 25, 45, 99, 120, 999], size=len(idx))
        child.loc[child.index[idx], "age"] = weird
        total += len(idx)
        log_injection("P02a", "child.age outside the 3-18 CHECK constraint",
                      ["child"], len(idx), "negative, zero, adult and sentinel ages (99/999)")

    # engagement_score outside 0..100
    idx = pick_indices(len(eng), r["p02_score_out_of_range_frac"])
    if len(idx):
        vals = np.where(rng.random(len(idx)) < 0.5,
                        rng.uniform(-40, -0.5, len(idx)),
                        rng.uniform(100.5, 350, len(idx)))
        eng.loc[eng.index[idx], "engagement_score"] = np.round(vals, 2)
        total += len(idx)
        log_injection("P02b", "engagement.engagement_score outside 0-100",
                      ["engagement"], len(idx), "negative scores and >100 scores")

    # task_completion_rate outside 0..100 (percent vs proportion confusion included)
    idx = pick_indices(len(eng), r["p02_completion_out_of_range_frac"])
    if len(idx):
        mode = rng.random(len(idx))
        vals = np.where(mode < 0.45, rng.uniform(101, 780, len(idx)),
                        np.where(mode < 0.75, rng.uniform(-60, -0.5, len(idx)),
                                 rng.uniform(0, 1, len(idx))))  # stored as proportion, not %
        eng.loc[eng.index[idx], "task_completion_rate"] = np.round(vals, 2)
        total += len(idx)
        log_injection("P02c", "engagement.task_completion_rate outside 0-100",
                      ["engagement"], len(idx), "values >100, negatives, and proportions (0-1)")

    # negative duration
    idx = pick_indices(len(act), r["p02_negative_duration_frac"])
    if len(idx):
        act.loc[act.index[idx], "duration"] = -np.abs(
            pd.to_numeric(act.loc[act.index[idx], "duration"], errors="coerce").to_numpy())
        total += len(idx)
        log_injection("P02d", "activity.duration negative", ["activity"], len(idx),
                      "sign-flipped durations from a faulty ETL transform")

    # negative response time
    idx = pick_indices(len(eng), r["p02_negative_response_time_frac"])
    if len(idx):
        eng.loc[eng.index[idx], "response_time"] = np.round(-rng.uniform(0.5, 12, len(idx)), 2)
        total += len(idx)
        log_injection("P02e", "engagement.response_time negative", ["engagement"], len(idx),
                      "clock-skew between recording devices")

    # negative attention shifts
    idx = pick_indices(len(eng), r["p02_negative_shifts_frac"])
    if len(idx):
        eng.loc[eng.index[idx], "number_of_attention_shifts"] = -rng.integers(1, 9, len(idx))
        total += len(idx)
        log_injection("P02f", "engagement.number_of_attention_shifts negative",
                      ["engagement"], len(idx), "counter reset artefacts")

    # negative device counts
    idx = pick_indices(len(child), r["p02_negative_devices_frac"])
    if len(idx):
        child.loc[child.index[idx], "number_of_devices"] = -rng.integers(1, 5, len(idx))
        total += len(idx)
        log_injection("P02g", "child.number_of_devices negative", ["child"], len(idx),
                      "-1 / -2 used as an undocumented 'unknown' sentinel")

    # contradictory: no tech access but several devices reported
    mask = (pd.to_numeric(child["access_to_technology"], errors="coerce") == 0)
    cand = np.flatnonzero(mask.to_numpy())
    if len(cand):
        take = rng.choice(cand, size=max(1, int(len(cand) * 0.06)), replace=False)
        child.loc[child.index[take], "number_of_devices"] = rng.integers(2, 7, len(take))
        total += len(take)
        log_injection("P02h", "Contradiction: access_to_technology = 0 but number_of_devices > 0",
                      ["child"], len(take), "cross-field logical contradiction")

    tables["child"] = child
    tables["engagement"] = eng
    tables["activity"] = act
    return tables


def inject_problem_03_unit_scale_inconsistency(tables, state):
    """
    P03 - Multi-source ingestion bug: some organizations logged lifestyle durations in
    MINUTES while the schema expects HOURS. Detection task: rows whose 7 lifestyle
    columns sum to far more than 24 hours.
    """
    r = ERROR_RATES
    lifestyle = tables["lifestyle"]
    co = state["co_hidden"]
    org_ids = tables["organization"]["org_id"].to_numpy()

    n_bad_orgs = max(1, int(len(org_ids) * r["p03_orgs_logging_minutes_frac"]))
    bad_orgs = rng.choice(org_ids, size=n_bad_orgs, replace=False)
    state["minute_logging_orgs"] = bad_orgs

    affected_children = co.loc[co["org_id"].isin(bad_orgs), "child_id"].unique()
    keep = rng.random(len(affected_children)) < r["p03_child_affected_frac"]
    affected_children = affected_children[keep]

    ls_idx = lifestyle.index[lifestyle["child_id"].isin(affected_children)]
    if len(ls_idx) == 0:
        log_injection("P03", "Unit/scale inconsistency (minutes logged as hours)",
                      ["lifestyle"], 0, "no children matched")
        return tables

    full_cols = ["screen_time", "gaming", "social_media_usage"]
    partial_cols = ["screen_time", "gaming"]

    partial_mask = rng.random(len(ls_idx)) < r["p03_partial_columns_prob"]
    full_idx = ls_idx[~partial_mask]
    part_idx = ls_idx[partial_mask]

    for col in full_cols:
        lifestyle.loc[full_idx, col] = np.round(
            pd.to_numeric(lifestyle.loc[full_idx, col], errors="coerce") * 60.0, 2)
    for col in partial_cols:
        lifestyle.loc[part_idx, col] = np.round(
            pd.to_numeric(lifestyle.loc[part_idx, col], errors="coerce") * 60.0, 2)

    time_cols = ["sleep_duration", "screen_time", "gaming", "social_media_usage",
                 "outdoor_time", "study_time", "free_play"]
    totals = lifestyle[time_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1, min_count=1)
    over_24 = int((totals > 24).sum())

    tables["lifestyle"] = lifestyle
    log_injection("P03", "Unit/scale inconsistency: minutes stored in decimal-hour columns",
                  ["lifestyle", "organization", "child_organization"], len(ls_idx),
                  f"{n_bad_orgs} orgs affected; {len(full_idx)} children with 3 columns in minutes, "
                  f"{len(part_idx)} with a partial (2-column) conversion; "
                  f"{over_24} lifestyle rows now sum to more than 24h/day")
    return tables


def inject_problem_04_duplicate_children(tables, state):
    """
    P04 - Duplicate / near-duplicate children: the same real child registered again at a
    different organization with misspelled name variants. Their lifestyle, enrolment and
    engagement rows are duplicated too (fuzzy-matching / entity-resolution problem).
    """
    r = ERROR_RATES
    child = tables["child"]
    lifestyle = tables["lifestyle"]
    co = tables["child_organization"]
    eng = tables["engagement"]
    act = tables["activity"]

    n_dup = int(len(child) * r["p04_duplicate_children_frac"])
    if n_dup == 0:
        return tables

    src_pos = rng.choice(len(child), size=n_dup, replace=False)
    src = child.iloc[src_pos].copy().reset_index(drop=True)

    next_id = int(child["child_id"].max()) + 1
    new_ids = np.arange(next_id, next_id + n_dup, dtype=np.int64)

    exact_mask = rng.random(n_dup) < r["p04_exact_duplicate_share"]
    new_fname, new_lname = [], []
    for i in range(n_dup):
        f = src.at[i, "fname"]
        l = src.at[i, "lname"]
        if exact_mask[i]:
            new_fname.append(f)
            new_lname.append(l)
        else:
            new_fname.append(misspell(f) if rng.random() < 0.75 else f)
            new_lname.append(misspell(l) if rng.random() < 0.6 else l)

    dup_child = src.copy()
    dup_child["child_id"] = new_ids
    dup_child["fname"] = new_fname
    dup_child["lname"] = new_lname
    # small independent reporting noise on the socio-economic answers
    dup_child["household_income"] = np.round(
        pd.to_numeric(dup_child["household_income"], errors="coerce")
        * rng.uniform(0.88, 1.15, n_dup), 2)
    dup_child["parents_working_hours"] = np.round(
        pd.to_numeric(dup_child["parents_working_hours"], errors="coerce")
        + rng.normal(0, 2.5, n_dup), 1)
    jitter_age = np.where(exact_mask, 0, rng.choice([-1, 0, 0, 1], size=n_dup))
    dup_child["age"] = np.clip(pd.to_numeric(dup_child["age"], errors="coerce") + jitter_age, 3, 18)

    tables["child"] = pd.concat([child, dup_child], ignore_index=True)

    # duplicated lifestyle rows (slightly different self-reports)
    ls_src = lifestyle[lifestyle["child_id"].isin(src["child_id"])].copy()
    id_map = dict(zip(src["child_id"].to_numpy(), new_ids))
    ls_dup = ls_src.copy()
    ls_dup["child_id"] = ls_dup["child_id"].map(id_map)
    ls_dup = ls_dup.dropna(subset=["child_id"])
    for c in ["sleep_duration", "screen_time", "gaming", "social_media_usage",
              "outdoor_time", "study_time", "free_play"]:
        ls_dup[c] = np.round(np.clip(
            pd.to_numeric(ls_dup[c], errors="coerce") * rng.uniform(0.85, 1.18, len(ls_dup)),
            0, None), 1)
    ls_dup["child_id"] = ls_dup["child_id"].astype(np.int64)
    tables["lifestyle"] = pd.concat([lifestyle, ls_dup], ignore_index=True)

    # enrol the twin in a DIFFERENT organization
    org_ids = tables["organization"]["org_id"].to_numpy()
    existing = co.groupby("child_id")["org_id"].apply(set).to_dict()
    new_co_rows = []
    for old_id, new_id in zip(src["child_id"].to_numpy(), new_ids):
        taken = existing.get(old_id, set())
        choices = np.setdiff1d(org_ids, np.array(sorted(taken), dtype=np.int64))
        if len(choices) == 0:
            choices = org_ids
        chosen = int(rng.choice(choices))
        base_date = co.loc[co["child_id"] == old_id, "enrollment_date"]
        base = pd.to_datetime(base_date.iloc[0]) if len(base_date) else pd.Timestamp(ENROLLMENT_START)
        new_co_rows.append({
            "child_id": int(new_id),
            "org_id": chosen,
            "enrollment_date": base + pd.Timedelta(days=int(rng.integers(20, 300))),
        })
    co_dup = pd.DataFrame(new_co_rows)
    tables["child_organization"] = pd.concat([co, co_dup], ignore_index=True)

    # a handful of engagement rows for each twin, inside the new org
    act_by_org = act.groupby("org_id")["activity_id"].apply(np.array).to_dict()
    per = int(r["p04_sessions_per_duplicate"])
    rows = []
    next_sid = int(eng["session_id"].max()) + 1
    src_scores = eng.groupby("child_id")["engagement_score"].mean()
    for i, (new_id, row) in enumerate(zip(new_ids, co_dup.itertuples(index=False))):
        acts = act_by_org.get(row.org_id)
        if acts is None or len(acts) == 0:
            continue
        base_mean = float(src_scores.get(src.at[i, "child_id"], 62.0))
        if not np.isfinite(base_mean):
            base_mean = 62.0
        k = int(rng.integers(max(2, per - 3), per + 4))
        chosen_acts = rng.choice(acts, size=k)
        day_offsets = rng.integers(3, 400, k)
        for j in range(k):
            score = float(np.clip(base_mean + rng.normal(0, 6.0), 0, 100))
            att = float(np.clip(rng.normal(300 + 3.0 * score, 90), 5, 3600))
            rows.append({
                "session_id": next_sid,
                "child_id": int(new_id),
                "activity_id": int(chosen_acts[j]),
                "session_date": pd.to_datetime(row.enrollment_date) + pd.Timedelta(days=int(day_offsets[j])),
                "avg_attention_duration": round(att, 2),
                "response_time": round(float(np.clip(rng.gamma(3, 0.9), 0.2, 45)), 2),
                "task_completion_rate": round(float(np.clip(0.78 * score + rng.normal(10, 7), 0, 100)), 2),
                "engagement_score": round(score, 2),
                "number_of_attention_shifts": int(np.clip(rng.poisson(9), 0, 60)),
                "_session_ord": int(pd.to_datetime(row.enrollment_date).toordinal() + int(day_offsets[j])),
                "_org_id": int(row.org_id),
                "_pair_idx": -1,
            })
            next_sid += 1
    if rows:
        tables["engagement"] = pd.concat([eng, pd.DataFrame(rows)], ignore_index=True)

    state["duplicate_pairs"] = list(zip(src["child_id"].to_numpy().tolist(), new_ids.tolist()))
    log_injection("P04", "Duplicate / near-duplicate children (entity resolution problem)",
                  ["child", "lifestyle", "child_organization", "engagement"],
                  n_dup,
                  f"{int(exact_mask.sum())} exact-name duplicates, {n_dup - int(exact_mask.sum())} "
                  f"fuzzy misspellings; each twin also has lifestyle, enrolment and "
                  f"{len(rows)} engagement rows")
    return tables


def inject_problem_05_missing_data_mechanisms(tables, state):
    """
    P05 - Missing data with THREE different mechanisms:
      * MNAR : household_income missing because low-income (and very-high-income) families skip it
      * MCAR : parents_working_hours missing completely at random
      * MAR  : number_of_devices missingness depends on school_type (observed variable)
      * MCAR : sporadic nulls in the lifestyle diary
    """
    r = ERROR_RATES
    child = tables["child"]
    lifestyle = tables["lifestyle"]

    income = pd.to_numeric(child["household_income"], errors="coerce").to_numpy(dtype="float64")
    valid = np.isfinite(income)
    q = np.nanquantile(income[valid], [0.25, 0.90]) if valid.any() else [0, 0]

    p_miss = np.full(len(child), r["p05_income_mnar_base"])
    p_miss = np.where(income <= q[0], r["p05_income_mnar_low_income"], p_miss)
    p_miss = np.where(income >= q[1], r["p05_income_mnar_high_income"], p_miss)
    mnar_mask = rng.random(len(child)) < p_miss
    child.loc[child.index[mnar_mask], "household_income"] = np.nan
    log_injection("P05a", "MNAR missingness in household_income",
                  ["child"], int(mnar_mask.sum()),
                  "low-income households skip the income question (45%), very-high-income skip "
                  "for privacy (18%), baseline 5% - missingness depends on the unobserved value")

    mcar_mask = rng.random(len(child)) < r["p05_working_hours_mcar"]
    child.loc[child.index[mcar_mask], "parents_working_hours"] = np.nan
    log_injection("P05b", "MCAR missingness in parents_working_hours",
                  ["child"], int(mcar_mask.sum()), "uniform random non-response")

    st = child["school_type"].to_numpy()
    p_dev = np.select(
        [st == "Public", st == "Private", st == "Homeschool", st == "International"],
        [r["p05_devices_mar_public"], r["p05_devices_mar_private"],
         r["p05_devices_mar_homeschool"], r["p05_devices_mar_international"]],
        default=0.08,
    )
    mar_mask = rng.random(len(child)) < p_dev
    child.loc[child.index[mar_mask], "number_of_devices"] = np.nan
    log_injection("P05c", "MAR missingness in number_of_devices",
                  ["child"], int(mar_mask.sum()),
                  "missingness fully explained by the observed school_type "
                  "(homeschool 20% / public 14% / international 5% / private 3%)")

    ls_cols = ["sleep_duration", "screen_time", "gaming", "social_media_usage",
               "outdoor_time", "study_time", "free_play"]
    n_ls = 0
    for c in ls_cols:
        m = rng.random(len(lifestyle)) < r["p05_lifestyle_mcar"]
        lifestyle.loc[lifestyle.index[m], c] = np.nan
        n_ls += int(m.sum())
    log_injection("P05d", "MCAR missingness in the lifestyle diary columns",
                  ["lifestyle"], n_ls, "incomplete diary entries across all 7 time columns")

    # --- observer / sensor drop-outs in the engagement measurements (MCAR)
    eng = tables["engagement"]
    n_eng = 0
    for c, rate in (("avg_attention_duration", r["p05_engagement_metric_mcar"]),
                    ("response_time", r["p05_engagement_metric_mcar"]),
                    ("task_completion_rate", r["p05_engagement_metric_mcar"] * 0.7),
                    ("number_of_attention_shifts", r["p05_engagement_metric_mcar"] * 0.5),
                    ("engagement_score", r["p05_engagement_metric_mcar"] * 0.4)):
        idx = pick_indices(len(eng), rate)
        if len(idx):
            eng.loc[eng.index[idx], c] = np.nan
            n_eng += len(idx)
    tables["engagement"] = eng
    log_injection("P05e", "Observer / sensor drop-outs in the engagement measurements",
                  ["engagement"], n_eng,
                  "MCAR gaps in avg_attention_duration, response_time, task_completion_rate, "
                  "number_of_attention_shifts and engagement_score")

    # --- incomplete activity catalogue entries
    act = tables["activity"]
    n_act = 0
    for c, rate in (("group_size", r["p05_activity_meta_missing"]),
                    ("movement_required", r["p05_activity_meta_missing"]),
                    ("duration", r["p05_activity_meta_missing"] * 0.6),
                    ("name", r["p05_activity_meta_missing"] * 0.3)):
        idx = pick_indices(len(act), rate)
        if len(idx):
            act.loc[act.index[idx], c] = np.nan
            n_act += len(idx)
    tables["activity"] = act
    log_injection("P05f", "Incomplete activity catalogue metadata",
                  ["activity"], n_act,
                  "activities registered without group_size, movement_required, duration or name")

    tables["child"] = child
    tables["lifestyle"] = lifestyle
    return tables


def inject_problem_06_timestamp_session_logic(tables, state):
    """
    P06 - Timestamp / session logic errors:
      * session_date earlier than the child's enrollment_date
      * multiple overlapping sessions for the same child on the same day
      * impossible future dates and NULL session dates
    """
    r = ERROR_RATES
    eng = tables["engagement"]
    n = len(eng)

    # --- sessions before enrolment
    idx = pick_indices(n, r["p06_session_before_enrollment_frac"])
    if len(idx):
        pos = eng.index[idx]
        shift = rng.integers(20, 500, len(idx))
        eng.loc[pos, "session_date"] = pd.to_datetime(eng.loc[pos, "session_date"]) \
            - pd.to_timedelta(shift, unit="D")
        log_injection("P06a", "session_date before the child's enrollment_date",
                      ["engagement", "child_organization"], len(idx),
                      "back-dated sessions from a retrospective paper-form import")

    # --- overlapping sessions: same child, same day, near-identical metrics
    idx = pick_indices(len(eng), r["p06_overlapping_sessions_frac"])
    if len(idx):
        clones = eng.iloc[idx].copy()
        next_sid = int(eng["session_id"].max()) + 1
        clones["session_id"] = np.arange(next_sid, next_sid + len(clones), dtype=np.int64)
        for c in ["avg_attention_duration", "response_time", "task_completion_rate",
                  "engagement_score"]:
            jitter = rng.normal(1.0, 0.02, len(clones))
            clones[c] = np.round(pd.to_numeric(clones[c], errors="coerce") * jitter, 2)
        # same day, different activity of the same org -> physically overlapping sessions
        eng = pd.concat([eng, clones], ignore_index=True)
        log_injection("P06b", "Overlapping / duplicated sessions on the same day for the same child",
                      ["engagement"], len(clones),
                      "double-logged sessions with near-identical measurements")

    # --- future dates
    idx = pick_indices(len(eng), r["p06_future_session_frac"])
    if len(idx):
        pos = eng.index[idx]
        eng.loc[pos, "session_date"] = pd.Timestamp(SESSION_END) + pd.to_timedelta(
            rng.integers(30, 2_000, len(idx)), unit="D")
        log_injection("P06c", "Impossible future session_date values",
                      ["engagement"], len(idx), "dates after the end of the collection window")

    # --- NULL session dates
    idx = pick_indices(len(eng), r["p06_null_session_date_frac"])
    if len(idx):
        eng.loc[eng.index[idx], "session_date"] = pd.NaT
        log_injection("P06d", "NULL session_date", ["engagement"], len(idx),
                      "sessions logged without a date")

    tables["engagement"] = eng.reset_index(drop=True)
    return tables


def inject_problem_07_categorical_encoding(tables, state):
    """
    P07 - Inconsistent categorical encoding across source systems:
    'Group' / 'group' / 'GRP' / 'group work', plus whitespace and null-like tokens.
    """
    r = ERROR_RATES
    activity = tables["activity"]
    child = tables["child"]
    total = 0

    act_cols = ["interaction_type", "activity_format", "participation_type", "difficulty_level"]
    for col in act_cols:
        idx = pick_indices(len(activity), r["p07_activity_cat_frac"])
        if len(idx):
            pos = activity.index[idx]
            vals = activity.loc[pos, col].to_numpy()
            activity.loc[pos, col] = [dirty_category(v) for v in vals]
            total += len(idx)

    child_cols = ["environment_type", "school_type"]
    for col in child_cols:
        idx = pick_indices(len(child), r["p07_child_cat_frac"])
        if len(idx):
            pos = child.index[idx]
            vals = child.loc[pos, col].to_numpy()
            child.loc[pos, col] = [dirty_category(v) for v in vals]
            total += len(idx)

    # whitespace noise
    for df, cols in ((activity, act_cols), (child, child_cols)):
        for col in cols:
            idx = pick_indices(len(df), r["p07_whitespace_frac"])
            if len(idx):
                pos = df.index[idx]
                pad = [" " * int(rng.integers(1, 3)) for _ in range(len(pos))]
                df.loc[pos, col] = [f"{p}{v}{p}" for p, v in zip(pad, df.loc[pos, col].astype(str))]
                total += len(idx)

    # null-like tokens
    n_tokens = 0
    for df, cols in ((activity, act_cols), (child, child_cols)):
        for col in cols:
            idx = pick_indices(len(df), r["p07_null_like_frac"])
            if len(idx):
                df.loc[df.index[idx], col] = rng.choice(NULL_LIKE_TOKENS, size=len(idx))
                n_tokens += len(idx)

    tables["activity"] = activity
    tables["child"] = child
    log_injection("P07", "Inconsistent categorical encoding (free-text category columns)",
                  ["activity", "child"], total + n_tokens,
                  "case variants, abbreviations (GRP/IND/PHY), synonyms (Online/Offline), "
                  f"padded whitespace and {n_tokens} null-like tokens (N/A, unknown, -, ?)")
    return tables


def inject_problem_08_self_report_bias(tables, state):
    """
    P08 - Self-report / proxy-report bias in the lifestyle diary:
      * teenagers systematically UNDER-report social media and gaming
      * parents of young children UNDER-estimate screen_time
      * heaping of answers on whole and half hours (recall bias)
    Detection hook: reported screen_time becomes inconsistent with number_of_devices.
    """
    r = ERROR_RATES
    lifestyle = tables["lifestyle"]
    child = tables["child"][["child_id", "age"]].copy()
    merged = lifestyle[["child_id"]].merge(child, on="child_id", how="left")
    ages = pd.to_numeric(merged["age"], errors="coerce").to_numpy(dtype="float64")

    teen = np.nan_to_num(ages, nan=0) >= 13
    young = np.nan_to_num(ages, nan=99) < 13

    teen_mask = teen & (rng.random(len(lifestyle)) < r["p08_teen_underreport_frac"])
    if teen_mask.any():
        shrink = 1 - rng.uniform(0.10, r["p08_teen_underreport_strength"], int(teen_mask.sum()))
        pos = lifestyle.index[teen_mask]
        for c in ("social_media_usage", "gaming"):
            lifestyle.loc[pos, c] = np.round(
                pd.to_numeric(lifestyle.loc[pos, c], errors="coerce") * shrink, 1)
    log_injection("P08a", "Teen self-report bias: under-reported social_media_usage / gaming",
                  ["lifestyle"], int(teen_mask.sum()),
                  "13-18 year olds shrink their reported usage by 10-45%")

    parent_mask = young & (rng.random(len(lifestyle)) < r["p08_parent_underreport_frac"])
    if parent_mask.any():
        shrink = 1 - rng.uniform(0.05, r["p08_parent_underreport_strength"], int(parent_mask.sum()))
        pos = lifestyle.index[parent_mask]
        lifestyle.loc[pos, "screen_time"] = np.round(
            pd.to_numeric(lifestyle.loc[pos, "screen_time"], errors="coerce") * shrink, 1)
    log_injection("P08b", "Proxy-report bias: parents under-estimate young children's screen_time",
                  ["lifestyle", "child"], int(parent_mask.sum()),
                  "creates an inconsistency between number_of_devices and reported screen_time")

    heap_cols = ["sleep_duration", "screen_time", "outdoor_time", "study_time", "free_play"]
    n_heap = 0
    for c in heap_cols:
        m = rng.random(len(lifestyle)) < r["p08_rounding_heaping_frac"]
        pos = lifestyle.index[m]
        vals = pd.to_numeric(lifestyle.loc[pos, c], errors="coerce")
        lifestyle.loc[pos, c] = np.round(vals * 2) / 2.0     # heap onto .0 / .5
        n_heap += int(m.sum())
    log_injection("P08c", "Recall bias: answer heaping on whole and half hours",
                  ["lifestyle"], n_heap, "digit preference in retrospective 'typical day' reports")

    tables["lifestyle"] = lifestyle
    return tables


def inject_problem_09_measurement_drift(tables, state):
    """
    P09 - Instrument / observer measurement drift across organizations.
    Different orgs use different (unvalidated) rating rubrics, so engagement_score and
    avg_attention_duration are not comparable across orgs: additive offsets, range
    compression, and some orgs logging attention in MINUTES instead of seconds.
    """
    r = ERROR_RATES
    eng = tables["engagement"]
    act = tables["activity"][["activity_id", "org_id"]]
    org_ids = tables["organization"]["org_id"].to_numpy()

    n_drift = max(1, int(len(org_ids) * r["p09_drifting_orgs_frac"]))
    drift_orgs = rng.choice(org_ids, size=n_drift, replace=False)
    lo, hi = r["p09_offset_range"]
    clo, chi = r["p09_compression_range"]
    offsets = dict(zip(drift_orgs, rng.uniform(lo, hi, n_drift)))
    scales = dict(zip(drift_orgs, rng.uniform(clo, chi, n_drift)))

    merged = eng[["session_id", "activity_id"]].merge(act, on="activity_id", how="left")
    org_col = merged["org_id"].to_numpy()
    off = np.array([offsets.get(o, 0.0) if pd.notna(o) else 0.0 for o in org_col])
    sca = np.array([scales.get(o, 1.0) if pd.notna(o) else 1.0 for o in org_col])

    score = pd.to_numeric(eng["engagement_score"], errors="coerce").to_numpy(dtype="float64")
    grand_mean = np.nanmean(score)
    new_score = grand_mean + (score - grand_mean) * sca + off
    changed = int((~np.isclose(np.nan_to_num(new_score), np.nan_to_num(score))).sum())
    eng["engagement_score"] = np.round(new_score, 2)

    # a few orgs logged attention duration in minutes instead of seconds
    n_unit = max(1, int(len(org_ids) * r["p09_attention_unit_orgs_frac"]))
    unit_orgs = set(rng.choice(org_ids, size=n_unit, replace=False).tolist())
    unit_mask = np.array([o in unit_orgs if pd.notna(o) else False for o in org_col])
    att = pd.to_numeric(eng["avg_attention_duration"], errors="coerce").to_numpy(dtype="float64")
    att = np.where(unit_mask, np.round(att / 60.0, 2), att)
    eng["avg_attention_duration"] = np.round(att, 2)

    state["drift_orgs"] = drift_orgs
    tables["engagement"] = eng
    log_injection("P09", "Instrument / observer measurement drift between organizations",
                  ["engagement", "organization"], changed + int(unit_mask.sum()),
                  f"{n_drift} orgs with rubric offsets ({lo}..{hi} points) and range compression "
                  f"({clo}..{chi}x); {n_unit} orgs logged avg_attention_duration in minutes "
                  f"({int(unit_mask.sum())} rows) - a construct-validity threat")
    return tables


def inject_problem_10_confounded_difficulty(tables, state):
    """
    P10 - Confounded difficulty ratings: some organizations systematically inflate or
    deflate difficulty_level without any change to the underlying activity design, so
    'Hard' is not comparable across orgs.
    """
    r = ERROR_RATES
    act = tables["activity"]
    org_ids = tables["organization"]["org_id"].to_numpy()

    n_orgs = max(1, int(len(org_ids) * r["p10_relabeling_orgs_frac"]))
    chosen = rng.choice(org_ids, size=n_orgs, replace=False)
    inflate = set(chosen[: n_orgs // 2].tolist())        # call everything harder
    deflate = set(chosen[n_orgs // 2:].tolist())         # call everything easier

    up = {"Easy": "Medium", "Medium": "Hard", "Hard": "Hard"}
    down = {"Hard": "Medium", "Medium": "Easy", "Easy": "Easy"}

    mask_rows = act["org_id"].isin(inflate | deflate).to_numpy()
    pick = mask_rows & (rng.random(len(act)) < r["p10_relabel_frac_within_org"])
    pos = act.index[pick]
    new_vals = []
    for org, val in zip(act.loc[pos, "org_id"].to_numpy(), act.loc[pos, "difficulty_level"].to_numpy()):
        if val not in DIFFICULTY_LEVELS:
            new_vals.append(val)
        elif org in inflate:
            new_vals.append(up[val])
        else:
            new_vals.append(down[val])
    act.loc[pos, "difficulty_level"] = new_vals

    tables["activity"] = act
    state["difficulty_inflating_orgs"] = inflate
    state["difficulty_deflating_orgs"] = deflate
    log_injection("P10", "Confounded / non-comparable difficulty_level across organizations",
                  ["activity", "organization"], int(pick.sum()),
                  f"{len(inflate)} orgs inflate difficulty, {len(deflate)} deflate it, while "
                  f"duration/group_size stay unchanged - cross-org difficulty analysis is invalid")
    return tables


def inject_problem_11_temporal_seasonal_confounding(tables, state):
    """
    P11 - Temporal / seasonal confounding: engagement_score depends on the month and the
    day of week rather than on the activity itself, and outdoor_time is season-dependent
    even though it is stored as a single 'typical day' value.
    """
    r = ERROR_RATES
    eng = tables["engagement"]
    dates = pd.to_datetime(eng["session_date"], errors="coerce")
    month = dates.dt.month.to_numpy(dtype="float64")
    weekday = dates.dt.weekday.to_numpy(dtype="float64")

    seasonal = np.zeros(len(eng))
    seasonal += np.where(np.isin(month, [6, 7, 8]), r["p11_summer_engagement_boost"], 0.0)
    seasonal += np.where(np.isin(month, [12, 1, 2]), r["p11_winter_engagement_penalty"], 0.0)
    seasonal += np.where(weekday >= 5, r["p11_weekend_boost"], 0.0)
    seasonal += np.where(np.isin(month, [5, 9]), -2.0, 0.0)      # exam months

    score = pd.to_numeric(eng["engagement_score"], errors="coerce").to_numpy(dtype="float64")
    eng["engagement_score"] = np.round(np.clip(score + seasonal, 0, 100), 2)

    att = pd.to_numeric(eng["avg_attention_duration"], errors="coerce").to_numpy(dtype="float64")
    eng["avg_attention_duration"] = np.round(att * (1 + seasonal / 120.0), 2)

    # lifestyle outdoor_time reflects the season in which the family was surveyed
    lifestyle = tables["lifestyle"]
    co = tables["child_organization"][["child_id", "enrollment_date"]].drop_duplicates("child_id")
    m = lifestyle[["child_id"]].merge(co, on="child_id", how="left")
    survey_month = pd.to_datetime(m["enrollment_date"], errors="coerce").dt.month.to_numpy(dtype="float64")
    mult = np.where(np.isin(survey_month, [6, 7, 8]), r["p11_summer_outdoor_multiplier"],
                    np.where(np.isin(survey_month, [12, 1, 2]), 0.62, 1.0))
    lifestyle["outdoor_time"] = np.round(
        pd.to_numeric(lifestyle["outdoor_time"], errors="coerce").to_numpy(dtype="float64") * mult, 1)

    tables["engagement"] = eng
    tables["lifestyle"] = lifestyle
    n_affected = int((seasonal != 0).sum()) + int((mult != 1.0).sum())
    log_injection("P11", "Temporal / seasonal confounding of engagement and outdoor time",
                  ["engagement", "lifestyle"], n_affected,
                  "summer +6 / winter -5 / weekend +3.5 points on engagement_score and a "
                  "season-dependent outdoor_time, with no season column available to control for it")
    return tables


def inject_problem_12_survivorship_dropout(tables, state):
    """
    P12 - Survivorship bias: children who disengage stop generating engagement rows.
    Their scores decline just before they drop out, then every later session disappears,
    so naive 'engagement over time' trends are inflated.
    """
    r = ERROR_RATES
    eng = tables["engagement"]
    co = state["co_hidden"].reset_index(drop=True)

    pair_idx = eng["_pair_idx"].to_numpy()
    valid = pair_idx >= 0
    dropout_ord = np.full(len(eng), np.inf)
    dropout_ord[valid] = co["dropout_ord"].to_numpy()[pair_idx[valid]]
    sess_ord = eng["_session_ord"].to_numpy(dtype="float64")

    # decline in the 60 days before dropping out
    pre = (sess_ord >= dropout_ord - 60) & (sess_ord < dropout_ord)
    if pre.any():
        prox = np.clip((sess_ord - (dropout_ord - 60)) / 60.0, 0, 1)
        score = pd.to_numeric(eng["engagement_score"], errors="coerce").to_numpy(dtype="float64")
        score = np.where(pre, np.clip(score - r["p12_pre_dropout_decline"] * prox, 0, 100), score)
        eng["engagement_score"] = np.round(score, 2)

    after = sess_ord > dropout_ord
    n_removed = int(after.sum())
    eng = eng.loc[~after].reset_index(drop=True)

    # the enrolment row itself stays -> no hint that the child stopped attending
    tables["engagement"] = eng
    n_pairs = int((co["dropout_ord"] < date_to_ordinal(SESSION_END)).sum())
    log_injection("P12", "Survivorship bias from silent drop-outs",
                  ["engagement", "child_organization"], n_removed,
                  f"{n_pairs} (child, org) enrolments end early; {n_removed} post-dropout sessions "
                  f"removed and {int(pre.sum())} pre-dropout sessions declined - "
                  f"child_organization still shows the enrolment as active")
    return tables


def inject_problem_13_outlier_distortion(tables, state):
    """
    P13 - Outlier-driven distortion: a small number of extreme rows (very young children
    with near-perfect engagement, plus absurd attention durations) concentrated in a few
    organizations, enough to flip org-level mean rankings.
    """
    r = ERROR_RATES
    eng = tables["engagement"]
    act = tables["activity"][["activity_id", "org_id"]]
    child = tables["child"]

    merged = eng[["session_id", "activity_id", "child_id"]].merge(act, on="activity_id", how="left")
    org_col = merged["org_id"].to_numpy()

    org_ids = tables["organization"]["org_id"].to_numpy()
    n_orgs = min(int(r["p13_outlier_orgs"]), len(org_ids))
    target_orgs = rng.choice(org_ids, size=n_orgs, replace=False)

    total = 0
    young_ids = child.loc[pd.to_numeric(child["age"], errors="coerce") <= 6, "child_id"].to_numpy()
    for org in target_orgs:
        cand = np.flatnonzero(org_col == org)
        if len(cand) == 0:
            continue
        k = min(int(r["p13_outliers_per_org"]), len(cand))
        take = rng.choice(cand, size=k, replace=False)
        pos = eng.index[take]
        eng.loc[pos, "engagement_score"] = np.round(rng.uniform(96.5, 100.0, k), 2)
        eng.loc[pos, "task_completion_rate"] = np.round(rng.uniform(97.0, 100.0, k), 2)
        eng.loc[pos, "number_of_attention_shifts"] = 0
        if len(young_ids):
            eng.loc[pos, "child_id"] = rng.choice(young_ids, size=k)
        total += k

    idx = pick_indices(len(eng), r["p13_extreme_attention_frac"])
    if len(idx):
        eng.loc[eng.index[idx], "avg_attention_duration"] = np.round(
            rng.uniform(9_000, 86_400, len(idx)), 2)   # longer than any possible session
        total += len(idx)

    tables["engagement"] = eng
    state["outlier_orgs"] = target_orgs
    log_injection("P13", "Outlier-driven distortion of organization-level rankings",
                  ["engagement", "organization", "child"], total,
                  f"{n_orgs} organizations polluted with near-perfect scores attributed to very "
                  f"young children, plus {len(idx)} impossible avg_attention_duration values "
                  f"(up to 24h) - mean vs trimmed-mean rankings will disagree")
    return tables


def inject_problem_14_nonresponse_tech_access(tables, state):
    """
    P14 - Non-representative device access / non-response bias.
    Low-income families who already skipped the income question also skip the technology
    questions, and some respondents over-report device ownership (social desirability),
    biasing any income -> tech access -> engagement conclusion.
    """
    r = ERROR_RATES
    child = tables["child"]
    ch_hidden = state["child_hidden"].set_index("child_id")

    true_income = ch_hidden["true_income"].reindex(child["child_id"]).to_numpy(dtype="float64")
    low_cut = np.nanquantile(true_income, 0.30)
    low_income = true_income <= low_cut

    mask = low_income & (rng.random(len(child)) < r["p14_low_income_tech_nonresponse"])
    pos = child.index[mask]
    child.loc[pos, "number_of_devices"] = np.nan
    child.loc[pos, "access_to_technology"] = np.nan
    log_injection("P14a", "Non-response bias on the technology questions (low-income households)",
                  ["child"], int(mask.sum()),
                  "the same households that hide income also hide tech access - any "
                  "income -> tech-access conclusion is biased by systematic non-response")

    over = (~mask) & (rng.random(len(child)) < r["p14_tech_overreport_frac"])
    pos = child.index[over]
    dev = pd.to_numeric(child.loc[pos, "number_of_devices"], errors="coerce")
    child.loc[pos, "number_of_devices"] = (dev.fillna(0) + rng.integers(1, 4, len(pos))).astype("float64")
    child.loc[pos, "access_to_technology"] = 1.0
    log_injection("P14b", "Social-desirability over-reporting of device ownership",
                  ["child"], int(over.sum()),
                  "inflated number_of_devices inconsistent with reported screen_time")

    tables["child"] = child
    return tables


def inject_problem_15_dirty_lookup_tables(tables, state):
    """
    P15 - The small reference tables are dirty too:
      * the same organization registered twice under a spelling variant (new org_id)
      * organization.type / preferences.category written as free text, sometimes missing
    Any GROUP BY on org type or preference category is now wrong.
    """
    r = ERROR_RATES
    org = tables["organization"]
    pref = tables["preferences"]

    # --- duplicated organizations (new surrogate key, same real-world entity)
    n_dup = int(len(org) * r["p15_duplicate_orgs_frac"])
    dup_rows = 0
    if n_dup:
        src = org.sample(n=n_dup, random_state=int(rng.integers(0, 10_000))).copy()
        next_id = int(org["org_id"].max()) + 1
        src["org_id"] = np.arange(next_id, next_id + n_dup, dtype=np.int64)
        variants = []
        for nm in src["name"].to_numpy():
            mode = int(rng.integers(0, 4))
            if mode == 0:
                variants.append(str(nm).upper())
            elif mode == 1:
                variants.append("  " + str(nm) + " ")
            elif mode == 2:
                variants.append(str(nm).replace(" ", "  "))
            else:
                variants.append(misspell(str(nm)))
        src["name"] = variants
        tables["organization"] = pd.concat([org, src], ignore_index=True)
        dup_rows = n_dup
        org = tables["organization"]

    # --- dirty / missing organization.type
    idx = pick_indices(len(org), r["p15_org_type_dirty_frac"])
    if len(idx):
        pos = org.index[idx]
        vals = org.loc[pos, "type"].astype(str).to_numpy()
        dirty = []
        for v in vals:
            mode = int(rng.integers(0, 4))
            if mode == 0:
                dirty.append(v.lower())
            elif mode == 1:
                dirty.append(v.upper())
            elif mode == 2:
                dirty.append(v.replace(" ", "_"))
            else:
                dirty.append(" " + v + "  ")
        org.loc[pos, "type"] = dirty
    idx2 = pick_indices(len(org), r["p15_org_type_missing_frac"])
    if len(idx2):
        repl = np.array(
            [np.nan if rng.random() < 0.5 else str(rng.choice(NULL_LIKE_TOKENS))
             for _ in range(len(idx2))], dtype=object)
        org.loc[org.index[idx2], "type"] = repl

    idx3 = pick_indices(len(org), r["p15_org_name_dirty_frac"])
    if len(idx3):
        pos = org.index[idx3]
        org.loc[pos, "name"] = [f"  {v} " if rng.random() < 0.5 else str(v).upper()
                                for v in org.loc[pos, "name"].astype(str)]
    tables["organization"] = org

    # --- duplicated preference options + dirty categories
    n_pdup = int(len(pref) * r["p15_duplicate_prefs_frac"])
    if n_pdup:
        src = pref.sample(n=n_pdup, random_state=int(rng.integers(0, 10_000))).copy()
        next_id = int(pref["preference_id"].max()) + 1
        src["preference_id"] = np.arange(next_id, next_id + n_pdup, dtype=np.int64)
        src["name"] = [str(v).lower() if rng.random() < 0.5 else "  " + str(v)
                       for v in src["name"]]
        pref = pd.concat([pref, src], ignore_index=True)

    idx = pick_indices(len(pref), r["p15_pref_category_dirty_frac"])
    if len(idx):
        pos = pref.index[idx]
        pref.loc[pos, "category"] = [
            str(v).upper() if rng.random() < 0.5 else str(v).replace("_", " ")
            for v in pref.loc[pos, "category"].astype(str)]
    idx = pick_indices(len(pref), r["p15_pref_category_missing_frac"])
    if len(idx):
        pref.loc[pref.index[idx], "category"] = np.nan
    tables["preferences"] = pref

    log_injection("P15", "Dirty reference/lookup tables (organization, preferences)",
                  ["organization", "preferences"],
                  dup_rows + n_pdup + len(idx3) + len(idx2),
                  f"{dup_rows} duplicate organizations under spelling variants, {n_pdup} duplicate "
                  f"preference options, free-text and missing type/category values - grouping by "
                  f"org type or preference category silently splits the same entity")
    return tables


def inject_problem_16_bridge_table_problems(tables, state):
    """
    P16 - Problems concentrated in the M:N bridge tables:
      * NULL, sentinel (1900-01-01) and future enrollment_date values
      * enrolments recorded AFTER the child's first session (back-office data entry)
      * child_preference rows pointing at preference options that do not exist
    """
    r = ERROR_RATES
    co = tables["child_organization"]
    cp = tables["child_preference"]
    eng = tables["engagement"]
    act = tables["activity"][["activity_id", "org_id"]]

    co["enrollment_date"] = to_datetime_safe(co["enrollment_date"])

    idx = pick_indices(len(co), r["p16_null_enrollment_date_frac"])
    n_null = len(idx)
    if n_null:
        co.loc[co.index[idx], "enrollment_date"] = pd.NaT

    idx = pick_indices(len(co), r["p16_sentinel_enrollment_frac"])
    n_sent = len(idx)
    if n_sent:
        sentinels = rng.choice([pd.Timestamp("1900-01-01"), pd.Timestamp("1970-01-01"),
                                pd.Timestamp("0001-01-01" if False else "1899-12-30")], size=n_sent)
        co.loc[co.index[idx], "enrollment_date"] = pd.to_datetime(sentinels)

    idx = pick_indices(len(co), r["p16_future_enrollment_frac"])
    n_fut = len(idx)
    if n_fut:
        co.loc[co.index[idx], "enrollment_date"] = pd.Timestamp(SESSION_END) + pd.to_timedelta(
            rng.integers(60, 1_500, n_fut), unit="D")

    # enrolment dated after the child's earliest session in that org
    first_sess = eng[["child_id", "activity_id", "session_date"]].merge(act, on="activity_id", how="left")
    first_sess["session_date"] = to_datetime_safe(first_sess["session_date"])
    first_sess = first_sess.groupby(["child_id", "org_id"], as_index=False)["session_date"].min()
    co2 = co.merge(first_sess, on=["child_id", "org_id"], how="left")
    cand = np.flatnonzero(co2["session_date"].notna().to_numpy())
    n_late = 0
    if len(cand):
        k = int(len(co) * r["p16_enrollment_after_sessions_frac"])
        k = min(k, len(cand))
        if k:
            take = rng.choice(cand, size=k, replace=False)
            co.loc[co.index[take], "enrollment_date"] = (
                co2.loc[take, "session_date"] + pd.to_timedelta(rng.integers(10, 400, k), unit="D")
            ).to_numpy()
            n_late = k

    # child_preference pointing at non-existent preference options
    max_pref = int(tables["preferences"]["preference_id"].max())
    idx = pick_indices(len(cp), r["p16_bad_preference_id_frac"])
    n_badpref = len(idx)
    if n_badpref:
        cp.loc[cp.index[idx], "preference_id"] = rng.integers(max_pref + 50, max_pref + 900, n_badpref)

    tables["child_organization"] = co
    tables["child_preference"] = cp
    log_injection("P16", "Bridge-table integrity and date problems",
                  ["child_organization", "child_preference"],
                  n_null + n_sent + n_fut + n_late + n_badpref,
                  f"{n_null} NULL enrolment dates, {n_sent} sentinel dates (1900/1970), {n_fut} future "
                  f"enrolments, {n_late} enrolments dated after the child's first session, "
                  f"{n_badpref} child_preference rows with an unknown preference_id")
    return tables


def inject_problem_17_duplicate_row_ingestion(tables, state):
    """
    P17 - A CSV batch was ingested twice: exact duplicate ROWS appear in five tables,
    including duplicated primary keys (same child_id, same session_id) and duplicated
    composite keys in both bridge tables.
    """
    r = ERROR_RATES
    spec = [
        ("child", "p17_dup_child_rows_frac", ["child_id"]),
        ("lifestyle", "p17_dup_lifestyle_rows_frac", ["child_id"]),
        ("engagement", "p17_dup_engagement_rows_frac", ["session_id"]),
        ("child_organization", "p17_dup_child_org_rows_frac", ["child_id", "org_id"]),
        ("child_preference", "p17_dup_child_pref_rows_frac", ["child_id", "preference_id"]),
    ]
    expected = {}
    total = 0
    detail = []
    for tname, rate_key, keys in spec:
        df = tables[tname]
        idx = pick_indices(len(df), ERROR_RATES[rate_key])
        if len(idx):
            clones = df.iloc[idx].copy()
            df = pd.concat([df, clones], ignore_index=True)
            tables[tname] = df
            total += len(idx)
            detail.append(f"{tname}: {len(idx)}")
        expected[tname] = int(tables[tname][keys].duplicated().sum())
    state["expected_duplicate_keys"] = expected
    tables["_expected_duplicate_keys"] = expected
    log_injection("P17", "Double ingestion: exact duplicate rows with duplicated primary keys",
                  [t for t, _, _ in spec], total,
                  "re-run of a load job; " + ", ".join(detail))
    return tables


def inject_problem_18_format_chaos(tables, state):
    """
    P18 - Free-text / format chaos produced by exporting from several different systems:
      * session_date and enrollment_date written in mixed formats (and impossible dates)
      * household_income stored as text with currency symbols, separators and 'approx'
      * names padded, upper-cased, blanked or replaced by 'Unknown'
      * a few numeric measurement cells polluted with text tokens
    Every affected column loads as `object` dtype, so nothing aggregates without cleaning.
    """
    r = ERROR_RATES
    eng = tables["engagement"]
    co = tables["child_organization"]
    child = tables["child"]

    def scramble_dates(series, frac):
        s = to_datetime_safe(series)
        out = s.dt.strftime("%Y-%m-%d").astype(object)
        out[s.isna()] = np.nan
        idx = pick_indices(len(out), frac)
        n_changed = 0
        for i in idx:
            v = s.iloc[i]
            mode = int(rng.integers(0, 6))
            if pd.isna(v):
                if mode < 3:
                    out.iloc[i] = rng.choice(["", "N/A", "unknown", "0000-00-00"])
                    n_changed += 1
                continue
            if mode == 0:
                out.iloc[i] = v.strftime("%d/%m/%Y")
            elif mode == 1:
                out.iloc[i] = v.strftime("%m-%d-%Y")
            elif mode == 2:
                out.iloc[i] = v.strftime("%d %b %Y")
            elif mode == 3:
                out.iloc[i] = v.strftime("%Y/%m/%d %H:%M:%S")
            elif mode == 4:
                out.iloc[i] = v.strftime("%B %d, %Y")
            else:
                out.iloc[i] = rng.choice(["31/02/2025", "2025-13-05", "0000-00-00", "N/A"])
            n_changed += 1
        return out, n_changed

    eng["session_date"], n_sess = scramble_dates(eng["session_date"], r["p18_mixed_session_date_frac"])
    co["enrollment_date"], n_enr = scramble_dates(co["enrollment_date"], r["p18_mixed_enrollment_date_frac"])

    # --- household_income as text
    inc = child["household_income"].astype(object)
    idx = pick_indices(len(child), r["p18_income_as_text_frac"])
    n_inc = 0
    for i in idx:
        v = inc.iloc[i]
        if pd.isna(v):
            continue
        mode = int(rng.integers(0, 5))
        if mode == 0:
            inc.iloc[i] = f"${float(v):,.0f}"
        elif mode == 1:
            inc.iloc[i] = f"{float(v):,.2f}"
        elif mode == 2:
            inc.iloc[i] = f"{float(v):.0f} EGP"
        elif mode == 3:
            inc.iloc[i] = f"approx {float(v)/1000:.1f}k"
        else:
            inc.iloc[i] = str(float(v)).replace(".", ",")
        n_inc += 1
    child["household_income"] = inc

    # --- dirty / missing names
    n_name = 0
    for col in ("fname", "lname"):
        idx = pick_indices(len(child), r["p18_name_dirty_frac"])
        pos = child.index[idx]
        vals = child.loc[pos, col].astype(str).to_numpy()
        dirty = []
        for v in vals:
            mode = int(rng.integers(0, 5))
            if mode == 0:
                dirty.append(v.upper())
            elif mode == 1:
                dirty.append("  " + v + "   ")
            elif mode == 2:
                dirty.append(v.lower())
            elif mode == 3:
                dirty.append("Unknown")
            else:
                dirty.append("")
            n_name += 1
        child.loc[pos, col] = dirty
        idx = pick_indices(len(child), r["p18_name_missing_frac"])
        child.loc[child.index[idx], col] = np.nan
        n_name += len(idx)

    # --- text tokens inside numeric measurement columns
    n_num = 0
    for tname, col in (("engagement", "response_time"), ("engagement", "avg_attention_duration"),
                       ("activity", "duration"), ("lifestyle", "screen_time")):
        df = tables[tname]
        s = df[col].astype(object)
        idx = pick_indices(len(df), r["p18_numeric_as_text_frac"])
        if len(idx):
            tokens = rng.choice(["n/a", "not recorded", "<1", ">60", "--", "TBD"], size=len(idx))
            s.iloc[idx] = tokens
            df[col] = s
            tables[tname] = df
            n_num += len(idx)

    tables["engagement"] = eng
    tables["child_organization"] = co
    tables["child"] = child
    log_injection("P18", "Free-text / format chaos across exported columns",
                  ["engagement", "child_organization", "child", "activity", "lifestyle"],
                  n_sess + n_enr + n_inc + n_name + n_num,
                  f"{n_sess} session dates and {n_enr} enrolment dates in mixed/impossible formats, "
                  f"{n_inc} incomes stored as currency text, {n_name} dirty or blank name cells, "
                  f"{n_num} numeric cells holding text tokens - all of these columns now load as "
                  f"object dtype")
    return tables


def run_all_injections(tables, state):
    """
    Order matters: structural deletions happen LAST so nothing re-introduces valid keys.
    """
    banner("INJECTING DATA-QUALITY PROBLEMS")
    steps = [
        ("P12 survivorship / drop-outs", inject_problem_12_survivorship_dropout),
        ("P04 duplicate & near-duplicate children", inject_problem_04_duplicate_children),
        ("P11 temporal / seasonal confounding", inject_problem_11_temporal_seasonal_confounding),
        ("P09 instrument / observer drift", inject_problem_09_measurement_drift),
        ("P10 confounded difficulty ratings", inject_problem_10_confounded_difficulty),
        ("P08 self-report / proxy-report bias", inject_problem_08_self_report_bias),
        ("P03 unit / scale inconsistency", inject_problem_03_unit_scale_inconsistency),
        ("P14 tech-access non-response bias", inject_problem_14_nonresponse_tech_access),
        ("P05 missing-data mechanisms (MCAR/MAR/MNAR)", inject_problem_05_missing_data_mechanisms),
        ("P07 inconsistent categorical encoding", inject_problem_07_categorical_encoding),
        ("P02 impossible / contradictory values", inject_problem_02_impossible_values),
        ("P06 timestamp / session logic errors", inject_problem_06_timestamp_session_logic),
        ("P13 outlier-driven distortion", inject_problem_13_outlier_distortion),
        ("P01 referential / structural integrity gaps", inject_problem_01_referential_integrity),
        ("P15 dirty lookup tables", inject_problem_15_dirty_lookup_tables),
        ("P16 bridge-table problems", inject_problem_16_bridge_table_problems),
        ("P17 duplicate row ingestion", inject_problem_17_duplicate_row_ingestion),
        ("P18 free-text / format chaos", inject_problem_18_format_chaos),
    ]
    for label, fn in steps:
        t0 = time.time()
        tables = fn(tables, state)
        print(f"  {label:<48s} done in {time.time() - t0:5.2f}s")
    return tables


# ==========================================================================================
# 8. DATA-QUALITY REPORT
# ==========================================================================================

def count_invalid_values(tables, table_name):
    """Count cells/rows that break a domain rule in one table."""
    df = tables[table_name]
    n = 0
    if table_name == "child":
        age = pd.to_numeric(df["age"], errors="coerce")
        n += int(((age < 3) | (age > 18)).sum())
        dev = pd.to_numeric(df["number_of_devices"], errors="coerce")
        n += int((dev < 0).sum())
        pwh = pd.to_numeric(df["parents_working_hours"], errors="coerce")
        n += int(((pwh < 0) | (pwh > 120)).sum())
        n += int((~df["environment_type"].isin(ENVIRONMENT_TYPES) & df["environment_type"].notna()).sum())
        n += int((~df["school_type"].isin(SCHOOL_TYPES) & df["school_type"].notna()).sum())
        acc = pd.to_numeric(df["access_to_technology"], errors="coerce")
        n += int(((acc == 0) & (dev > 0)).sum())
    elif table_name == "activity":
        dur = pd.to_numeric(df["duration"], errors="coerce")
        n += int((dur <= 0).sum())
        for col, allowed in (("interaction_type", INTERACTION_TYPES),
                             ("activity_format", ACTIVITY_FORMATS),
                             ("participation_type", PARTICIPATION_TYPES),
                             ("difficulty_level", DIFFICULTY_LEVELS)):
            n += int((~df[col].isin(allowed) & df[col].notna()).sum())
    elif table_name == "engagement":
        for col in ("engagement_score", "task_completion_rate"):
            v = pd.to_numeric(df[col], errors="coerce")
            n += int(((v < 0) | (v > 100)).sum())
        rt = pd.to_numeric(df["response_time"], errors="coerce")
        n += int((rt < 0).sum())
        sh = pd.to_numeric(df["number_of_attention_shifts"], errors="coerce")
        n += int((sh < 0).sum())
        att = pd.to_numeric(df["avg_attention_duration"], errors="coerce")
        n += int(((att < 0) | (att > 7_200)).sum())
        d = to_datetime_safe(df["session_date"])
        n += int((d > pd.Timestamp(SESSION_END)).sum())
        n += int((d.isna() & df["session_date"].notna()).sum())
    elif table_name == "lifestyle":
        cols = ["sleep_duration", "screen_time", "gaming", "social_media_usage",
                "outdoor_time", "study_time", "free_play"]
        vals = df[cols].apply(pd.to_numeric, errors="coerce")
        n += int((vals < 0).sum().sum())
        n += int((vals.sum(axis=1, min_count=1) > 24).sum())
        n += int((vals["sleep_duration"] > 16).sum())
    elif table_name == "child_organization":
        d = to_datetime_safe(df["enrollment_date"])
        n += int((d.isna()).sum())
        n += int((d < pd.Timestamp("2000-01-01")).sum())
        n += int((d > pd.Timestamp(SESSION_END)).sum())
    return n


def business_key_columns(table_name):
    """Columns used for 'duplicate record' detection (excludes surrogate PKs)."""
    if table_name == "child":
        return ["fname", "lname", "age", "family_size", "environment_type", "school_type"]
    if table_name == "engagement":
        return ["child_id", "activity_id", "session_date"]
    if table_name == "activity":
        return ["org_id", "name", "duration", "group_size", "interaction_type"]
    if table_name == "organization":
        return ["name", "type"]
    if table_name == "preferences":
        return ["name", "category"]
    return EXPORT_COLUMNS[table_name]


def build_data_quality_report(tables):
    banner("BUILDING DATA-QUALITY REPORT")
    rows = []
    for t in TABLE_ORDER:
        df = tables[t][EXPORT_COLUMNS[t]]
        bkeys = [c for c in business_key_columns(t) if c in df.columns]
        rows.append({
            "section": "TABLE_SUMMARY",
            "table_name": t,
            "total_rows": len(df),
            "total_columns": df.shape[1],
            "missing_values": int(df.isna().sum().sum()),
            "columns_with_missing": int((df.isna().sum() > 0).sum()),
            "duplicate_records": int(df.duplicated(subset=bkeys).sum()),
            "invalid_values": count_invalid_values(tables, t),
            "foreign_key_violations": count_fk_violations(tables, t),
            "problem_id": "",
            "problem_name": "",
            "tables_affected": "",
            "records_affected": "",
            "details": f"duplicates evaluated on {bkeys}",
        })

    for rec in INJECTION_LOG:
        rows.append({
            "section": "INJECTED_PROBLEM",
            "table_name": "",
            "total_rows": "",
            "total_columns": "",
            "missing_values": "",
            "columns_with_missing": "",
            "duplicate_records": "",
            "invalid_values": "",
            "foreign_key_violations": "",
            "problem_id": rec["problem_id"],
            "problem_name": rec["problem_name"],
            "tables_affected": rec["tables_affected"],
            "records_affected": rec["records_affected"],
            "details": rec["details"],
        })

    report = pd.DataFrame(rows)
    print(report[report["section"] == "TABLE_SUMMARY"][
        ["table_name", "total_rows", "missing_values", "duplicate_records",
         "invalid_values", "foreign_key_violations"]].to_string(index=False))
    print(f"\n{len(INJECTION_LOG)} intentional data-quality problems logged.")
    return report


# ==========================================================================================
# 9. EXPORT
# ==========================================================================================

INT_LIKE_COLUMNS = {
    "child": ["child_id", "age", "family_size", "num_of_siblings",
              "access_to_technology", "number_of_devices"],
    "organization": ["org_id"],
    "activity": ["activity_id", "org_id", "duration", "group_size", "movement_required"],
    "preferences": ["preference_id"],
    "child_organization": ["child_id", "org_id"],
    "child_preference": ["child_id", "preference_id"],
    "engagement": ["session_id", "child_id", "activity_id", "number_of_attention_shifts"],
    "lifestyle": ["child_id"],
}


def print_messiness_summary(tables):
    """Show, per table, how much of the exported data is actually dirty."""
    banner("MESSINESS SUMMARY (share of rows touched by at least one problem)")
    print(f"{'table':<20s}{'rows':>10s}{'% rows w/ NULL':>16s}{'% dirty cells':>15s}"
          f"{'dup key rows':>14s}{'FK violations':>15s}")
    for t in TABLE_ORDER:
        df = tables[t][EXPORT_COLUMNS[t]]
        rows_with_null = float((df.isna().any(axis=1)).mean() * 100)
        dirty_cells = float(df.isna().sum().sum() / max(df.size, 1) * 100)
        dup_keys = int(df[PRIMARY_KEYS[t]].duplicated().sum())
        fk = count_fk_violations(tables, t)
        print(f"{t:<20s}{len(df):>10,}{rows_with_null:>15.1f}%{dirty_cells:>14.1f}%"
              f"{dup_keys:>14,}{fk:>15,}")
    obj_cols = []
    for t in TABLE_ORDER:
        df = tables[t][EXPORT_COLUMNS[t]]
        for c in df.columns:
            if pd.api.types.is_object_dtype(df[c]) and c not in ("name", "type", "category",
                                                                 "fname", "lname"):
                obj_cols.append(f"{t}.{c}")
    if obj_cols:
        print("\nColumns that no longer parse as a clean type (need cleaning): "
              + ", ".join(obj_cols))


def export_tables(tables, report, output_dir=OUTPUT_DIR):
    banner("EXPORTING CSV FILES")
    os.makedirs(output_dir, exist_ok=True)

    for t in TABLE_ORDER:
        df = tables[t][EXPORT_COLUMNS[t]].copy()
        for c in INT_LIKE_COLUMNS.get(t, []):
            # skip columns that P18 polluted with text tokens, otherwise the cast would
            # silently erase the injected problem
            if c in df.columns and not pd.api.types.is_object_dtype(df[c]):
                df[c] = safe_int_series(df[c])
        for dcol in ("session_date", "enrollment_date"):
            if dcol in df.columns and pd.api.types.is_datetime64_any_dtype(df[dcol]):
                df[dcol] = df[dcol].dt.strftime("%Y-%m-%d")
        path = os.path.join(output_dir, f"{t}.csv")
        df.to_csv(path, index=False, float_format=CSV_FLOAT_FORMAT)
        print(f"  {path:<45s} {len(df):>8,} rows x {df.shape[1]} cols")

    rpath = os.path.join(output_dir, "data_quality_report.csv")
    report.to_csv(rpath, index=False)
    print(f"  {rpath:<45s} {len(report):>8,} rows")
    return output_dir


# ==========================================================================================
# 10. MAIN
# ==========================================================================================

def main():
    t_start = time.time()
    set_seeds(SEED)

    apply_messiness_level(MESSINESS_LEVEL)
    print(f"Messiness level: {MESSINESS_LEVEL}  (seed {SEED})")

    banner("1) GENERATING PARENT TABLES")
    organization, org_hidden = generate_organization(N_ORGANIZATIONS)
    print(f"  organization        : {len(organization):>8,} rows")
    child, child_hidden = generate_child(N_CHILDREN, org_hidden)
    print(f"  child               : {len(child):>8,} rows")
    preferences = generate_preferences(N_PREFERENCES)
    print(f"  preferences         : {len(preferences):>8,} rows")

    banner("2) GENERATING DEPENDENT TABLES & RELATIONSHIPS")
    activity, activity_hidden = generate_activity(organization, org_hidden, N_ACTIVITIES)
    print(f"  activity            : {len(activity):>8,} rows")
    child_organization, co_hidden = generate_child_organization(child, organization, org_hidden)
    print(f"  child_organization  : {len(child_organization):>8,} rows")
    child_preference, child_pref_hidden = generate_child_preference(child, preferences)
    print(f"  child_preference    : {len(child_preference):>8,} rows")
    lifestyle, lifestyle_hidden = generate_lifestyle(child, child_hidden)
    print(f"  lifestyle           : {len(lifestyle):>8,} rows")
    engagement = generate_engagement(child, child_hidden, child_pref_hidden, lifestyle,
                                     activity, activity_hidden, organization, org_hidden,
                                     co_hidden, N_ENGAGEMENT)
    print(f"  engagement          : {len(engagement):>8,} rows")

    tables = {
        "organization": organization,
        "child": child,
        "preferences": preferences,
        "activity": activity,
        "child_organization": child_organization,
        "child_preference": child_preference,
        "lifestyle": lifestyle,
        "engagement": engagement,
    }
    state = {
        "org_hidden": org_hidden,
        "child_hidden": child_hidden,
        "activity_hidden": activity_hidden,
        "co_hidden": co_hidden,
        "child_pref_hidden": child_pref_hidden,
        "lifestyle_hidden": lifestyle_hidden,
    }

    # ------------------------------------------------------------------ clean validation
    clean_issues = run_validation(tables, stage="CLEAN DATASET (must be perfect)", strict=True)

    # ------------------------------------------------------------------ injection
    tables = run_all_injections(tables, state)

    # ------------------------------------------------------------------ final validation
    final_issues = run_validation(tables, stage="FINAL DATASET (issues below are INTENTIONAL)")
    sanity_check_final(tables)

    # ------------------------------------------------------------------ report + export
    report = build_data_quality_report(tables)
    print_messiness_summary(tables)
    export_tables(tables, report, OUTPUT_DIR)

    banner("DONE")
    print(f"Seed               : {SEED}")
    print(f"Clean-stage issues : {len(clean_issues)} (expected 0)")
    print(f"Final-stage issues : {len(final_issues)} (all intentional, see data_quality_report.csv)")
    print(f"Injected problems  : {len(INJECTION_LOG)}")
    print(f"Total runtime      : {time.time() - t_start:.1f}s")
    print(f"Output folder      : {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()
