# -*- coding: utf-8 -*-
"""Patched for Clinic_Dataplatform (Dataproc-friendly).

Changes vs original:
- IO only: read input from BigQuery staging table (or optional CSV), and write output mapping to BigQuery.
- Normalization rules are kept as-is (copied into main()).

Run on Dataproc:

spark-submit \
  --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
  gen_master_drug_code_medicines_script_patched.py \
  --input-bq-table wata-clinicdataplatform-gcp.bq_stage.public_medicines_cdc \
  --output-bq-table wata-clinicdataplatform-gcp.master.master_drug_map_v1 \
  --temp-gcs-bucket <YOUR_TEMP_BUCKET>

Note: This script converts Spark DF -> Pandas for rule verification, so keep data volume reasonable.
"""

import argparse
import re
import unicodedata
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

# Try multiple providers to survive connector/classpath differences across clusters.
BQ_DATA_SOURCES = [
    "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider",
    "com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider",
    "com.google.cloud.spark.bigquery",
    "bigquery",
]

def load_input_df(
    spark: SparkSession,
    input_bq_table: str,
    input_csv: str | None,
):
    cols = ["id", "name", "active_element", "concentration", "manufacturer"]
    if input_csv:
        return (
            spark.read
            .option("header", "true")
            .csv(input_csv)
            .select(*cols)
        )

    last_error = None
    df_spark = None
    for source in BQ_DATA_SOURCES:
        try:
            df_spark = (
                spark.read.format(source)
                .option("table", input_bq_table)
                .load()
                .select(*cols)
            )
            break
        except Exception as exc:
            last_error = exc
    if df_spark is None:
        raise RuntimeError(f"Cannot read BigQuery table with known providers: {last_error}")

    return df_spark

def write_output_to_bigquery(
    sdf,
    output_bq_table: str,
    temp_gcs_bucket: str,
    mode: str = "overwrite",
) -> None:
    last_error = None
    for source in BQ_DATA_SOURCES:
        try:
            (
                sdf.write.format(source)
                .option("table", output_bq_table)
                .option("temporaryGcsBucket", temp_gcs_bucket)
                .mode(mode)
                .save()
            )
            return
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Cannot write BigQuery table with known providers: {last_error}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-bq-table", default="wata-clinicdataplatform-gcp.silver_curated.public_medicines")
    parser.add_argument("--output-bq-table", default="wata-clinicdataplatform-gcp.master.master_drug_map_v1")
    parser.add_argument("--temp-gcs-bucket", required=True)
    parser.add_argument("--input-csv", default=None)
    parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append"])
    args = parser.parse_args()

    spark = SparkSession.builder.appName("master_drug_code_medicines_patch").getOrCreate()
    df = load_input_df(
        spark=spark,
        input_bq_table=args.input_bq_table,
        input_csv=args.input_csv,
    ).select(
        *[
            F.col(c).cast("string").alias(c)
            for c in ["id", "name", "active_element", "concentration", "manufacturer"]
        ]
    )

    # =========================
    # FINAL VERSION (CLEAN)
    # =========================

    # ---------- Base utils ----------
    def strip_accents(s: str) -> str:
        if s is None:
            return ""
        s = str(s).replace("đ", "d").replace("Đ", "D")
        s = unicodedata.normalize("NFKD", s)
        return "".join(ch for ch in s if not unicodedata.combining(ch))

    def norm_spaces(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "")).strip()

    PLACEHOLDERS = {"", "-", "--", "na", "nan", "none", "null"}
    def is_blank(x) -> bool:
        if x is None:
            return True
        return str(x).strip().lower() in PLACEHOLDERS

    def clean_key(s: str) -> str:
        return norm_spaces(strip_accents(s).lower())


    # ---------- Dosage ----------
    DOSAGE_RE = re.compile(
        r"""(?ix)
        (?:
            \d+(?:[.,]\d+)?\s*(?:mg|g|mcg|µg|ug|iu|ui|ml|l|%|meq|mmol|dv|đv)
            (?:\s*/\s*\d+(?:[.,]\d+)?\s*(?:ml|l))?
        )
        """,
    )

    def normalize_dose(d: str) -> str:
        d = re.sub(r"\s+", "", (d or "")).upper().replace("UI", "IU").replace(",", ".")
        d = re.sub(r"[^A-Z0-9\+\/\%\.\-]", "", d)
        d = d.replace("/1ML", "/ML")  # optional convention
        return d

    def extract_first_dosage(text: str) -> str:
        if text is None:
            return ""
        m = DOSAGE_RE.search(str(text))
        return normalize_dose(m.group(0)) if m else ""

    def extract_all_dosages(text: str):
        if text is None:
            return []
        out = []
        for m in DOSAGE_RE.findall(str(text)):
            x = normalize_dose(m)
            if x:
                out.append(x)
        return out

    def remove_dosages(text: str) -> str:
        if text is None:
            return ""
        return norm_spaces(DOSAGE_RE.sub(" ", str(text)))

    def looks_like_only_dosage(text: str) -> bool:
        if text is None:
            return False
        s = normalize_dose(str(text).strip())
        if not s:
            return False
        parts = re.split(r"\+", s)
        return all(DOSAGE_RE.fullmatch(p) for p in parts if p)


    # ---------- Noise ----------
    NOISE_PHRASES = sorted([
        "tinh dau",
        "moi lo chua", "mỗi lọ chứa",
        "moi vien chua", "mỗi viên chứa",
        "moi goi chua", "mỗi gói chứa",
        "moi dung dich", "mỗi dung dịch",
        "duoi dang", "dưới dạng",
        "dang", "dạng",
        "dung dich", "dung dịch",
        "vi hat", "vỉ hạt",
        "chua", "chứa",
        "moi", "mỗi",
        "td", "dl",
        "thien nhien", "thiên nhiên", "tu nhien", "tự nhiên", "acid",
        "tuong ung", "tương ứng", "tương đương", "tuong duong",
    ], key=len, reverse=True)

    def remove_noise_phrases(s: str) -> str:
        if s is None:
            return ""
        x = strip_accents(str(s)).lower()
        for ph in NOISE_PHRASES:
            ph2 = strip_accents(ph).lower().strip()
            x = re.sub(rf"\b{re.escape(ph2)}\b", " ", x)
        return norm_spaces(x)

    NON_ACTIVE_WORDS = {
        "tablet","tablets","tab","capsule","capsules","cap","syrup","solution","powder","granule","suspension",
        "inj","injection","ampoule","vial","drop","drops","spray","cream","gel","ointment",
        "oral","iv","im","sc","topical","film","coated","retard","sr","mr","xr",
        "vien","viên","ong","ống","lo","lọ","goi","gói","chai","hop","hộp","vỉ","vi",
        "mg","g","mcg","ug","iu","ui","ml","l","%",
        "hv","opc",
    }

    SALT_WORDS = {
        "citrat","citrate","hydrochloride","hcl","sulfate","sulphate","phosphate",
        "tartrate","maleate","mesylate","besylate","nitrate","acetate","acetat","succinate",
    }


    # ---------- Equivalent split ----------
    EQUIV_SPLIT_RE = re.compile(r"(?i)\btuong\s+duong\s+voi\b|\btương\s+đương\s+với\b")
    def drop_equivalent_rhs(s: str) -> str:
        if s is None:
            return ""
        parts = EQUIV_SPLIT_RE.split(str(s), maxsplit=1)
        return parts[0].strip() if parts else str(s).strip()


    # ---------- Brackets ----------
    BRACKET_FORM_PREFIX_RE = re.compile(
        r"""(?ix) ^
        (?:duoi\s+dang|dưới\s+dạng|dang|dạng|
           as|in\s+the\s+form\s+of|under\s+the\s+form\s+of|
           in\s+form\s+of|in\s+the\s+form)
        \b
        """
    )

    def split_outside_and_brackets(text: str):
        if text is None:
            return "", []
        s = str(text)
        br = re.findall(r"[\(\[\{]([^\)\]\}]+)[\)\]\}]", s)
        outside = re.sub(r"[\(\[\{][^\)\]\}]+[\)\]\}]", " ", s)
        outside = norm_spaces(outside)
        br = [norm_spaces(x) for x in br if norm_spaces(x)]
        return outside, br


    # ---------- Family rules (ONE SOURCE OF TRUTH) ----------
    FAMILY_RULES = [
        {"name":"vit_c",  "match_any":[r"\bvitamin\s*c\b", r"\bascorbic\b", r"\bacid\s*ascorbic\b"],
         "canonical":"ascorbic", "short":"ASCORB"},

        {"name":"vit_e",  "match_any":[r"\bvitamin\s*e\b", r"\bvitamine\b", r"\btocofer\w*\b", r"\btocopher\w*\b"],
         "canonical":"tocopheryl acetate", "short":"TOCOPH"},

        {"name":"vit_d3", "match_any":[r"\bvitamin\s*d3\b", r"\bcholecalciferol\b", r"\bcolecalciferol\b"],
         "canonical":"cholecalciferol", "short":"CHOLE"},

        {"name":"vit_a",  "match_any":[r"\bvitamin\s*a\b", r"\bretinol\b", r"\bretinyl\b"],
         "canonical":"retinol", "short":"RETIN"},

        {"name":"vit_b1", "match_any":[r"\bvitamin\s*b1\b", r"\bthiamin(e)?\b"],
         "canonical":"thiamine", "short":"THIAM"},

        {"name":"vit_b6", "match_any":[r"\bvitamin\s*b6\b", r"\bpyridox(in|ine)\b"],
         "canonical":"pyridoxine", "short":"PYRIDO"},

        {"name":"vit_b12","match_any":[r"\bvitamin\s*b12\b", r"\bcobalamin\b", r"\bcyanocobalamin\b"],
         "canonical":"cyanocobalamin", "short":"CYANO"},

        {"name":"vit_k1",
         "match_any":[r"\bvitamin\s*k1\b", r"\bphytomenadion(e)?\b", r"\bphytonadion(e)?\b"],
         "canonical":"phytomenadione", "short":"PHYTOM"},

        {"name":"vit_pp",
         "match_any":[r"\bvitamin\s*pp\b", r"\bnicotinamid(e)?\b", r"\bniacinamid(e)?\b"],
         "canonical":"nicotinamide", "short":"NICOTI"},
    ]

    def apply_family_rules(text: str):
        if text is None:
            return None
        t = strip_accents(str(text)).lower()
        t = re.sub(r"[^\w\s\-]", " ", t)
        t = norm_spaces(t)
        for rule in FAMILY_RULES:
            if any(re.search(pat, t, flags=re.I) for pat in rule["match_any"]):
                return rule["canonical"]
        return None

    def family_short_code(canonical: str):
        ca = (canonical or "").strip().lower()
        for rule in FAMILY_RULES:
            if rule["canonical"] == ca:
                return rule.get("short")
        return None

    # canonical -> code overrides (authoritative)
    ACTIVE_SHORT_OVERRIDES = {
        "paracetamol": "PARA",
        "ascorbic": "ASCORB",
        "tocopheryl acetate": "TOCOPH",
        "cholecalciferol": "CHOLE",
        "retinol": "RETIN",
        "thiamine": "THIAM",
        "pyridoxine": "PYRIDO",
        "cyanocobalamin": "CYANO",
        "phytomenadione": "PHYTOM",
        "nicotinamide": "NICOTI",
    }

    # canonical equivalences to unify duplicates
    CANON_EQUIV = [
        (re.compile(r"\bacetaminophen\b", re.I), "paracetamol"),
        (re.compile(r"\bpanadol\b", re.I), "paracetamol"),
        (re.compile(r"\bpara\b", re.I), "paracetamol"),
        (re.compile(r"\bnicotinamid\b", re.I), "nicotinamide"),   # IMPORTANT
        (re.compile(r"\bniacinamid\b", re.I), "nicotinamide"),    # IMPORTANT
    ]


    # ---------- Vietnamese code ----------
    VN_IGNORE = {
        "hoat","huyet","trung","uong",
        "extractum","extract","chiet","xuat","tu",
        "ty","ti","le","ratio",
        "10","1","10:1","1:1",
        "vien","ong","lo","goi","chai","hop","vi",
    }

    def vn_tokens_for_code(original_active: str):
        x = drop_equivalent_rhs(original_active)
        x = remove_dosages(x)
        x = remove_noise_phrases(x)
        x = norm_spaces(x)
        t = strip_accents(x).lower()
        t = re.sub(r"[^a-z0-9\s]", " ", t)
        t = norm_spaces(t)
        toks = [w for w in t.split() if w and w not in VN_IGNORE]
        return toks

    def vn_phrase_code(toks: list[str]) -> str:
        if not toks:
            return ""
        # 2-token phrase by default
        take = toks[:2] if len(toks) >= 2 else toks[:1]
        # "ban ha che" -> 3 token
        if len(toks) >= 3 and toks[0] == "ban" and toks[1] == "ha" and toks[2] == "che":
            take = toks[:3]
        return "".join(w.upper() for w in take)

    def vn_build_code(original_active: str) -> str:
        toks = vn_tokens_for_code(original_active)
        if not toks:
            return ""
        # cao rule
        if toks[0] == "cao":
            out = ["CAO"]
            i = 1
            if i < len(toks) and toks[i] in {"dac","kho","long","mem","hon","hop"}:
                out.append(toks[i].upper())
                i += 1
            name = toks[i:i+2] if len(toks) >= i+2 else [toks[-1]]
            out.extend([w.upper() for w in name])
            return "".join(out)
        return vn_phrase_code(toks)


    # ---------- Canonicalize ----------
    # broaden hard match to catch alphatocopheryl / tocopheryl
    TOCO_HARD_RE = re.compile(r"(?i)\btocofer\w*\b|\btocopher\w*\b|\btocopheryl\b|\balphatocopheryl\b")

    def canonicalize_active(original_phrase: str) -> str:
        s = drop_equivalent_rhs(original_phrase)
        s = remove_dosages(s)
        s = remove_noise_phrases(s)

        t0 = strip_accents(s).lower()

        # HARD: any tocopher/tocofer/tocopheryl/alphatocopheryl => tocopheryl acetate
        if TOCO_HARD_RE.search(t0):
            return "tocopheryl acetate"

        fam = apply_family_rules(s)
        if fam:
            return fam

        s = strip_accents(s).lower()
        s = re.sub(r"[^\w\s\-]", " ", s)
        s = norm_spaces(s)

        toks = [t for t in s.split() if t and t not in NON_ACTIVE_WORDS]
        toks = [t for t in toks if t not in SALT_WORDS]
        s = " ".join(toks)

        for pat, repl in CANON_EQUIV:
            s = pat.sub(repl, s)

        return norm_spaces(s)



    # ---------- Code generation ----------
    def make_active_code(canonical_active: str, original_seg: str) -> str:
        ca = (canonical_active or "").strip().lower()
        if not ca:
            return ""

        # ALWAYS prefer canonical overrides / family short first (fix vitamin E/PP)
        if ca in ACTIVE_SHORT_OVERRIDES:
            return ACTIVE_SHORT_OVERRIDES[ca]
        fam = family_short_code(ca)
        if fam:
            return fam

        # then Vietnamese phrase rule
        # (note: original_seg can contain VN but if canonical is vitamin family we already returned above)
        if strip_accents(original_seg) != str(original_seg):
            return vn_build_code(original_seg)

        # fallback
        first = re.split(r"\s+", ca)[0]
        first = re.sub(r"[^a-z0-9]", "", first)
        return first[:6].upper()


    # ---------- Split candidates ----------
    SEP_RE = re.compile(r"\s*(?:\+|;|,|/|\bva\b|\bvà\b|&)\s*", re.IGNORECASE)
    ALIAS_BRACKET_RE = re.compile(
        r"(?i)\bnicotinamid(e)?\b|\bniacinamid(e)?\b|\bvitamin\s*pp\b|"
        r"\bpyridox(in|ine)\b|\bthiamin(e)?\b|\bcobalamin\b|"
        r"\bcholecalciferol\b|\bretinol\b|\bretinyl\b|"
        r"\btocopheryl\b|\btocopher\w*\b|\btocofer\w*\b"
    )


    def split_candidates(text: str):
        outside, brs = split_outside_and_brackets(text)
        parts = []
        if outside:
            parts.append(outside)
        for b in brs:
            k = clean_key(b)
            if BRACKET_FORM_PREFIX_RE.search(k):
                continue
            if ALIAS_BRACKET_RE.search(strip_accents(b).lower()):
                continue
            parts.append(b)

        segments = []
        for p in parts:
            for seg in SEP_RE.split(p):
                seg = norm_spaces(seg)
                if seg:
                    segments.append(seg)
        return segments


    # ---------- Extract pairs (top2 + canonical dedupe) ----------
    def extract_pairs_top2(hoatchatchinh: str, hamluong: str, max_pairs: int = 2):
        if hoatchatchinh is None:
            return []

        hcc = drop_equivalent_rhs(str(hoatchatchinh))
        segments = split_candidates(hcc)
        fallback_doses = extract_all_dosages(hamluong) if not is_blank(hamluong) else []

        out = []
        used_canon = set()

        for seg_raw in segments:
            active_clean = norm_spaces(remove_noise_phrases(remove_dosages(seg_raw)))
            canon = canonicalize_active(active_clean) or ""

            # IMPORTANT: unify canon text BEFORE dedupe
            canon = strip_accents(canon).lower()
            canon = re.sub(r"\bnicotinamid\b", "nicotinamide", canon)
            canon = re.sub(r"\bniacinamid\b", "nicotinamide", canon)
            canon = norm_spaces(canon)

            if not canon or canon in used_canon:
                continue

            dose = extract_first_dosage(seg_raw)
            if not dose and len(out) < len(fallback_doses):
                dose = fallback_doses[len(out)]

            out.append((seg_raw, canon, dose))
            used_canon.add(canon)

            if len(out) >= max_pairs:
                break

        return out[:max_pairs]



    # ---------- Build mathuoc ----------
    def build_mathuoc(hcc, hl):

        # swapped fix
        if looks_like_only_dosage(hcc) and (hl is not None) and (not is_blank(hl)):
            hcc, hl = hl, hcc

        pairs = extract_pairs_top2(hcc, hl, max_pairs=2)  # returns (seg_raw, canon, dose)

        # Build (code, dose) then FINAL DEDUPE by code (prefer one with dose)
        tmp = []
        for seg_raw, canon, dose in pairs:
            code = make_active_code(canon, seg_raw)
            if code:
                tmp.append((code, normalize_dose(dose) if dose else "", canon, seg_raw))

        best_by_code = {}
        for code, dose, canon, seg_raw in tmp:
            if code not in best_by_code:
                best_by_code[code] = (code, dose, canon, seg_raw)
            else:
                # prefer the one with dose
                if not best_by_code[code][1] and dose:
                    best_by_code[code] = (code, dose, canon, seg_raw)

        kept = list(best_by_code.values())[:2]

        codes = [k[0] for k in kept]
        doses = [k[1] for k in kept if k[1]]

        active_code = "+".join(codes)
        dose_code = "+".join(doses[:2])

        mathuoc_new = f"{active_code}_{dose_code}" if (active_code and dose_code) else active_code

        pairs_dbg = [f"{canon}::{dose}" for _, dose, canon, _ in kept]

        return {
            "pairs_top2": " | ".join(pairs_dbg),
            "mathuoc_new": mathuoc_new
        }

    # ---------- Apply ----------
    output_schema = StructType([
        StructField("source_medicine_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("active_element", StringType(), True),
        StructField("concentration", StringType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("mathuoc_new", StringType(), True),
        StructField("manufacturer_code", StringType(), True),
        StructField("master_drug_code", StringType(), True),
    ])





    # ---------- Patterns to remove (ANYWHERE) ----------
    # Keep these as "phrases" (multi-word) or abbreviations that are not part of real brand names.
    REMOVE_ANYWHERE = [
        # VN org words that often appear in the middle too
        r"cong\s+ty", r"cty",
        r"chi\s+nhanh", r"van\s+phong\s+dai\s+dien", r"vpdd",
        r"trung\s+tam", r"nha\s+may", r"xuong", r"co\s+so",

        # VN legal forms
        r"tnhh\s*mtv", r"tnhh\s*1tv", r"tnhh", r"mtv", r"mot\s+thanh\s+vien",
        r"co\s+phan", r"cophan", r"\bcp\b",

        # EN legal forms (avoid removing standalone "co" to reduce false positives)
        r"ltd", r"limited", r"inc", r"incorporated", r"corp", r"corporation",
        r"llc", r"plc", r"llp", r"lp",

        # Common international legal forms (pharma data often has these)
        r"jsc", r"j\s+s\s+c",                # J.S.C => j s c
        r"gmbh", r"g\s+m\s+b\s+h",          # g.m.b.h => g m b h
        r"ges\s+m\s+b\s+h", r"m\s+b\s+h",   # ges.m.b.h => ges m b h
        r"kg", r"ag", r"sa", r"sarl", r"spa", r"s\s+p\s+a",
        r"bv", r"nv", r"oy", r"ab", r"as", r"aps", r"kft",

        # Manufacturing markers (usually not part of brand)
        r"nfg", r"mfg", r"manufacturing",
    ]

    REMOVE_ANYWHERE += [
        # Manufacturing / facility descriptors (VN)
        r"san\s+xuat",                          # sản xuất
        r"ban\s+thanh\s+pham",                  # bán thành phẩm
        r"dong\s+goi",                          # đóng gói
        r"so\s+cap",                            # sơ cấp
        r"thuong\s+mai",                        # thương mại
        r"gia\s+cong",                          # gia công
        r"phan\s+phoi",                         # phân phối
        r"nhap\s+khau",                         # nhập khẩu
        r"xuat\s+khau",                         # xuất khẩu

        # “facility” variants you showed
        r"co\s+so\s+san\s+xuat",                # cơ sở sản xuất
        r"nha\s+san\s+xuat",                    # nhà sản xuất
        r"xuong\s+san\s+xuat",                  # xưởng sản xuất
        r"noi\s+san\s+xuat",                    # nơi sản xuất
        r"dia\s+chi",                           # địa chỉ (often noise in your sample)

        r"\&\s*co",   # handles “& Co KG”
    ]

    REMOVE_ANYWHERE += [
        # Facility / organization type (VN)
        r"nha\s+may",           # nhà máy
        r"xi\s+nghiep",         # xí nghiệp
        r"tram", r"trung\s+tam",  # optional if you see these

        # Medical supplies words
        r"vat\s+tu\s+y\s+te",   # vật tư y tế

        # Packaging / production levels
        r"so\s+cap",            # sơ cấp
        r"thu\s+cap",           # thứ cấp
        r"dong\s+goi",          # đóng gói

        # Abbreviations seen in your data
        r"ttbyt",               # TTBYT
    ]

    REMOVE_ANYWHERE += [

        # Conjunction
        r"\bva\b",                              # và
        r"nha",

        # Packaging / production level phrases
        r"dong\s+goi\s+so\s+cap",               # đóng gói sơ cấp
        r"dong\s+goi\s+thu\s+cap",              # đóng gói thứ cấp
        r"xuat\s+xuong",                       # xuất xưởng
        r"xuat",
        r"thuoc",
        r"tai",
        # Medical equipment
        r"trang\s+thiet\s+bi\s+y\s+te",         # trang thiết bị y tế
        r"ttbyt",                              # abbreviation

        # Manufacturing business models
        r"nhan\s+gia\s+cong",                   # nhận gia công
        r"nhan\s+chuyen\s+giao\s+cong\s+nghe",  # nhận chuyển giao công nghệ

        # Tech descriptor
        r"cong\s+nghe\s+cao",                   # công nghệ cao
    ]

    ADDRESS_MARKERS = [
        r"dia\s+chi", r"address",
        r"street", r"st", r"road", r"rd", r"ave", r"avenue",
        r"blvd", r"boulevard", r"way", r"lane", r"ln",
        r"travessia", r"travesia",  # appears in your sample
    ]

    def strip_address_tail_keep_unit_numbers(s: str) -> str:
        # cut from address markers
        for m in ADDRESS_MARKERS:
            s = re.sub(rf"\b{m}\b.*$", " ", s)

        # remove 4+ digit numbers (postal / street)
        s = re.sub(r"\b\d{4,}\b", " ", s)

        # remove alphanumeric codes like A31B, 12AB
        s = re.sub(r"\b[a-z]*\d+[a-z]+\b|\b[a-z]+\d+[a-z]*\b", " ", s)

        s = re.sub(r"\s+", " ", s).strip()
        return s




    # Country words only if they are at the end
    COUNTRY_END = [
        r"viet\s+nam", r"vietnam",
        r"usa", r"u\s+s\s+a",
        r"japan", r"korea", r"china", r"taiwan", r"thailand",
        r"france", r"germany"
    ]

    def remove_duplicate_phrases(s: str) -> str:
        words = s.split()
        n = len(words)

        for size in range(n//2, 1, -1):
            first = words[:size]
            second = words[size:2*size]
            if first == second:
                return " ".join(first)

        return s


    def normalize_company_name(name) -> str:
        if pd.isna(name):
            return ""

        # 1) lowercase for processing
        s = str(name).lower()

        # 2) accent-safe (đ->d + remove diacritics)
        s = strip_accents(s)

        # 3) normalize punctuation early (so (TNHH), J.S.C, ... become tokens)
        s = re.sub(r"[^\w\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()

        # 4) remove prefixes/suffixes/stop phrases ANYWHERE (word-boundary safe)
        for ph in REMOVE_ANYWHERE:
            s = re.sub(rf"\b{ph}\b", " ", s)
        s = re.sub(r"\s+", " ", s).strip()

        # 5) remove country words if at end
        for c in COUNTRY_END:
            s = re.sub(rf"(?:\s+{c})$", "", s)
        s = re.sub(r"\s+", " ", s).strip()

        # 6) remove address tail & long digit tokens (but KEEP 1–3 digit unit numbers like 120/150)
        s = strip_address_tail_keep_unit_numbers(s)
        s = re.sub(r"\s+", " ", s).strip()

        # 7) remove duplicated contiguous phrases
        s = remove_duplicate_phrases(s)
        s = re.sub(r"\s+", " ", s).strip()

        # 8) title case output
        return s.title()


    def company_short_code(name) -> str:
        norm = normalize_company_name(name)
        if not norm:
            return ""

        words = norm.split()

        # Single word: first 4 letters (same as before)
        if len(words) == 1:
            return words[0][:4].upper()

        out = []
        for w in words:
            # Keep 1–3 digit "unit numbers" fully (120, 150, ...)
            if w.isdigit() and len(w) <= 3:
                out.append(w)
            else:
                out.append(w[0])

        return "".join(out).upper()


    def transform_partition(iterator):
        for pdf in iterator:
            if pdf.empty:
                yield pd.DataFrame(columns=[f.name for f in output_schema.fields])
                continue

            for c in ["id", "name", "active_element", "concentration", "manufacturer"]:
                if c not in pdf.columns:
                    pdf[c] = None
                pdf[c] = pdf[c].astype("string")

            out = pd.DataFrame(
                [build_mathuoc(hcc, hl) for hcc, hl in zip(pdf["active_element"], pdf["concentration"])],
                index=pdf.index,
            )
            df_out = pd.concat([pdf, out], axis=1)
            df_out["manufacturer_code"] = df_out["manufacturer"].apply(company_short_code)
            df_out["master_drug_code"] = (
                df_out["mathuoc_new"].fillna("").str.upper().str.strip()
                + "__" +
                df_out["manufacturer_code"].fillna("").str.upper().str.strip()
            ).str.strip("_")

            keep_cols = [
                "id",
                "name",
                "active_element",
                "concentration",
                "manufacturer",
                "mathuoc_new",
                "manufacturer_code",
                "master_drug_code",
            ]
            for c in keep_cols:
                if c not in df_out.columns:
                    df_out[c] = None

            df_final = df_out[keep_cols].copy().rename(columns={"id": "source_medicine_id"})
            for c in df_final.columns:
                df_final[c] = df_final[c].map(lambda v: None if pd.isna(v) else str(v))

            yield df_final

    result_df = df.mapInPandas(transform_partition, schema=output_schema)

    write_output_to_bigquery(
        sdf=result_df,
        output_bq_table=args.output_bq_table,
        temp_gcs_bucket=args.temp_gcs_bucket,
        mode=args.write_mode,
    )

    print(f"[OK] Wrote mapping rows to {args.output_bq_table}")

if __name__ == "__main__":
    main()


