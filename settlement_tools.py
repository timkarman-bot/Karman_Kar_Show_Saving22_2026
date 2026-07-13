import csv
import io
import re
import zipfile
from datetime import datetime, timezone
from html import escape
from typing import Any, Dict, Iterable, List, Optional


MONEY_RE = re.compile(r"[^0-9.\-()]")


def _norm_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def _first(row: Dict[str, Any], names: Iterable[str]) -> str:
    for name in names:
        key = _norm_header(name)
        if key in row and row[key] not in (None, ""):
            return str(row[key]).strip()
    return ""


def _money(value: Any) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    negative = raw.startswith("(") and raw.endswith(")")
    cleaned = MONEY_RE.sub("", raw).replace("(", "").replace(")", "")
    try:
        amount = float(cleaned or 0)
    except ValueError:
        return 0.0
    return -abs(amount) if negative else amount


def _classify(description: str, amount: float) -> str:
    text = (description or "").lower()
    if amount < 0 or "refund" in text:
        return "Refund"
    if any(term in text for term in ("vote", "voting", "people's choice", "peoples choice")):
        return "Vote"
    if any(term in text for term in ("registration", "register", "car entry", "car show")):
        return "Registration"
    if any(term in text for term in ("attendee", "attendance", "admission", "gate")):
        return "Admission"
    if any(term in text for term in ("sponsor", "sponsorship", "vendor", "booth")):
        return "Sponsor"
    return "Needs Review"


def parse_stripe_csv(csv_bytes: bytes, filename: str = "") -> Dict[str, Any]:
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    records: List[Dict[str, Any]] = []
    audit_flags: List[str] = []

    for idx, raw_row in enumerate(reader, start=2):
        row = {_norm_header(k): v for k, v in (raw_row or {}).items()}
        created = _first(row, ["created", "created date", "date", "date/time", "available on"])
        transaction_id = _first(
            row,
            [
                "id",
                "transaction id",
                "charge id",
                "payment id",
                "payment intent id",
                "balance transaction id",
            ],
        )
        payer = _first(row, ["customer email", "customer", "name", "card name", "payer", "description"])
        description = _first(
            row,
            [
                "description",
                "statement descriptor",
                "product name",
                "payment link",
                "metadata item",
                "metadata payment_item_type",
                "metadata",
            ],
        )
        gross = _money(
            _first(row, ["gross", "gross amount", "amount", "amount paid", "total", "converted amount"])
        )
        fee = abs(_money(_first(row, ["fee", "fees", "stripe fee", "processing fee"])))
        net_raw = _first(row, ["net", "net amount", "converted net"])
        net = _money(net_raw) if net_raw else gross - fee

        payment_type = _first(row, ["payment type", "type", "metadata payment_item_type"])
        if not payment_type:
            payment_type = _classify(" ".join([description, payer, transaction_id]), gross)
        else:
            payment_type = payment_type.strip().replace("_", " ").title()

        destination = "Voting_Log" if payment_type == "Vote" else "Revenue_Log"
        mapped = "N" if payment_type in {"Needs Review", "Refund"} else "Y"
        audit = ""
        if not transaction_id:
            audit = "Review: missing transaction ID"
        elif payment_type == "Needs Review":
            audit = "Review: needs classification"
        elif payment_type == "Refund":
            audit = "Review: refund/negative transaction"

        if audit:
            audit_flags.append(f"Row {idx}: {audit}")

        records.append(
            {
                "source": "Stripe",
                "source_file": filename,
                "date": created,
                "transaction_id": transaction_id,
                "payer": payer,
                "description": description,
                "gross": round(gross, 2),
                "fee": round(fee, 2),
                "net": round(net, 2),
                "payment_type": payment_type,
                "destination": destination,
                "mapped": mapped,
                "audit": audit,
            }
        )

    summary: Dict[str, Dict[str, float]] = {}
    for record in records:
        key = record["payment_type"]
        bucket = summary.setdefault(key, {"count": 0, "gross": 0.0, "fees": 0.0, "net": 0.0})
        bucket["count"] += 1
        bucket["gross"] += float(record["gross"])
        bucket["fees"] += float(record["fee"])
        bucket["net"] += float(record["net"])

    for bucket in summary.values():
        bucket["gross"] = round(bucket["gross"], 2)
        bucket["fees"] = round(bucket["fees"], 2)
        bucket["net"] = round(bucket["net"], 2)

    return {
        "filename": filename,
        "records": records,
        "summary": summary,
        "audit_flags": audit_flags,
        "row_count": len(records),
    }


def _cell_ref(row: int, col: int) -> str:
    letters = ""
    while col:
        col, rem = divmod(col - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row}"


def _sheet_xml(rows: List[List[Any]]) -> str:
    out = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">',
        "<sheetData>",
    ]
    for r_idx, row in enumerate(rows, start=1):
        out.append(f'<row r="{r_idx}">')
        for c_idx, value in enumerate(row, start=1):
            if value is None:
                continue
            ref = _cell_ref(r_idx, c_idx)
            if isinstance(value, dict) and "formula" in value:
                out.append(f'<c r="{ref}"><f>{escape(value["formula"])}</f></c>')
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                out.append(f'<c r="{ref}"><v>{value}</v></c>')
            else:
                out.append(
                    f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'
                )
        out.append("</row>")
    out.append("</sheetData></worksheet>")
    return "".join(out)


def _xlsx_bytes(sheets: Dict[str, List[List[Any]]]) -> bytes:
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        overrides = [
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        ]
        rels = []
        sheet_entries = []
        for idx, name in enumerate(sheets.keys(), start=1):
            overrides.append(
                f'<Override PartName="/xl/worksheets/sheet{idx}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            )
            rels.append(
                f'<Relationship Id="rId{idx}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{idx}.xml"/>'
            )
            sheet_entries.append(
                f'<sheet name="{escape(name)}" sheetId="{idx}" r:id="rId{idx}"/>'
            )

        zf.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            + "".join(overrides)
            + "</Types>",
        )
        zf.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            "</Relationships>",
        )
        zf.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            "<sheets>"
            + "".join(sheet_entries)
            + "</sheets></workbook>",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + "".join(rels)
            + "</Relationships>",
        )
        for idx, rows in enumerate(sheets.values(), start=1):
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", _sheet_xml(rows))
    mem.seek(0)
    return mem.getvalue()


def build_settlement_workbook(event: Dict[str, Any], records: List[Dict[str, Any]]) -> bytes:
    event_name = event.get("event_name") or "Karman Kar Show"
    event_date = event.get("event_date") or ""
    charity_name = event.get("charity_name") or ""
    promoter_share = float(event.get("promoter_share") or 0.7)
    charity_share = float(event.get("charity_share") or 0.3)
    voting_admin_fee = float(event.get("voting_admin_fee") or 0.1)

    processor_rows = [
        [
            "Import Source",
            "Date",
            "Transaction ID",
            "Payer / Description",
            "Gross Amount ($)",
            "Fee ($)",
            "Net Amount ($)",
            "Payment Type",
            "Mapped Log",
            "Mapped? (Y/N)",
            "Notes",
            "Suggested Destination",
            "Audit Flag",
        ]
    ]
    revenue_rows = [
        [
            "Date",
            "Revenue Type",
            "Payer / Source",
            "Description",
            "Qty",
            "Unit Price ($)",
            "Gross Amount ($)",
            "Payment Method",
            "Transaction / Receipt ID",
            "Collected By",
            "Deposit Account",
            "Deposit Date",
            "Paid? (Y/N)",
            "Verified Deposit? (Y/N)",
            "Include in Split? (Y/N)",
            "Charity Share %",
            "Charity Portion ($)",
            "Promoter Portion ($)",
            "Notes",
            "Audit Flag",
        ]
    ]
    voting_rows = [
        [
            "Date / Time",
            "Car Number",
            "Votes Purchased",
            "Amount Paid ($)",
            "Payment Method",
            "Transaction ID",
            "Donor Name",
            "Donor Email",
            "Processor Fee ($)",
            "Refunded? (Y/N)",
            "Net Voting Donation ($)",
            "Admin Fee Portion ($)",
            "Net to Charity from Voting ($)",
            "Processed By",
            "Notes",
            "Audit Flag",
        ]
    ]

    for record in records:
        processor_rows.append(
            [
                record["source"],
                record["date"],
                record["transaction_id"],
                record["payer"] or record["description"],
                record["gross"],
                record["fee"],
                record["net"],
                record["payment_type"],
                record["destination"],
                record["mapped"],
                "",
                record["destination"],
                record["audit"],
            ]
        )
        if record["payment_type"] == "Vote":
            row_number = len(voting_rows) + 1
            voting_rows.append(
                [
                    record["date"],
                    "",
                    "",
                    record["gross"],
                    "Stripe",
                    record["transaction_id"],
                    record["payer"],
                    "",
                    record["fee"],
                    "N",
                    {"formula": f"IF(J{row_number}=\"Y\",0,MAX(D{row_number}-I{row_number},0))"},
                    {"formula": f"K{row_number}*Event_Setup!B8"},
                    {"formula": f"MAX(K{row_number}-L{row_number},0)"},
                    "",
                    record["description"],
                    record["audit"],
                ]
            )
        elif record["payment_type"] != "Refund":
            row_number = len(revenue_rows) + 1
            revenue_rows.append(
                [
                    record["date"],
                    record["payment_type"],
                    record["payer"],
                    record["description"],
                    1,
                    record["gross"],
                    {"formula": f"E{row_number}*F{row_number}"},
                    "Stripe",
                    record["transaction_id"],
                    "",
                    "",
                    "",
                    "Y",
                    "Y",
                    "Y",
                    charity_share,
                    {"formula": f"IF(AND(M{row_number}=\"Y\",O{row_number}=\"Y\"),G{row_number}*P{row_number},0)"},
                    {"formula": f"IF(G{row_number}=\"\",0,G{row_number}-Q{row_number})"},
                    "",
                    record["audit"],
                ]
            )

    sheets = {
        "README": [
            ["Karman Kar Shows & Events - Automated Settlement Workbook"],
            ["Generated", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")],
            ["Purpose", "Stripe CSV import, settlement calculation, charity report, and audit review."],
            ["Note", "Review rows marked Needs Review or Refund before final settlement."],
        ],
        "Event_Setup": [
            ["Field", "Value"],
            ["Event Name", event_name],
            ["Event Date", event_date],
            ["Charity Legal Name", charity_name],
            ["Promoter Share of Net Event Revenue", promoter_share],
            ["Charity Share of Net Event Revenue", charity_share],
            ["Use % or Flat Voting Fee?", "Percent"],
            ["Voting Admin Fee %", voting_admin_fee],
        ],
        "Processor_Import": processor_rows,
        "Revenue_Log": revenue_rows,
        "Voting_Log": voting_rows,
        "Settlement_Calc": [
            ["Metric", "Amount / Value", "Source"],
            ["Gross Event Revenue Collected by Promoter", {"formula": "SUM(Revenue_Log!G2:G1000)"}, "Revenue_Log"],
            ["Verified Event Revenue", {"formula": 'SUMIFS(Revenue_Log!G2:G1000,Revenue_Log!M2:M1000,"Y",Revenue_Log!N2:N1000,"Y")'}, "Revenue_Log"],
            ["Approved Reimbursable Expenses", 0, "Manual expense log can be added"],
            ["Net Event Revenue", {"formula": "B3-B4"}, "Verified revenue minus expenses"],
            ["Promoter Share %", {"formula": "Event_Setup!B5"}, "Agreement"],
            ["Charity Share %", {"formula": "Event_Setup!B6"}, "Agreement"],
            ["Promoter Share of Net Event Revenue", {"formula": "B5*B6"}, "Calculated"],
            ["Charity Share of Net Event Revenue", {"formula": "B5*B7"}, "Calculated"],
            ["Total Voting Donations Paid", {"formula": "SUM(Voting_Log!D2:D1000)"}, "Voting_Log"],
            ["Voting Processor Fees", {"formula": "SUM(Voting_Log!I2:I1000)"}, "Voting_Log"],
            ["Net Voting Donations", {"formula": "SUM(Voting_Log!K2:K1000)"}, "Voting_Log"],
            ["Voting Admin Fee", {"formula": "SUM(Voting_Log!L2:L1000)"}, "Agreement"],
            ["Net Voting Donations to Charity", {"formula": "SUM(Voting_Log!M2:M1000)"}, "Voting_Log"],
            ["FINAL TOTAL TO CHARITY", {"formula": "B9+B14"}, "Event charity share + voting charity share"],
            ["TOTAL TO PROMOTER", {"formula": "B8+B13"}, "Promoter share + voting admin fee"],
        ],
        "Dashboard": [
            ["KPI", "Value"],
            ["Gross Revenue", {"formula": "Settlement_Calc!B2"}],
            ["Verified Revenue", {"formula": "Settlement_Calc!B3"}],
            ["Net Revenue", {"formula": "Settlement_Calc!B5"}],
            ["Voting Donations Paid", {"formula": "Settlement_Calc!B10"}],
            ["Stripe/Processor Fees", {"formula": "Settlement_Calc!B11"}],
            ["Charity Total", {"formula": "Settlement_Calc!B15"}],
            ["Promoter Total", {"formula": "Settlement_Calc!B16"}],
            ["Outstanding Audit Flags", {"formula": 'COUNTIF(Processor_Import!M2:M1000,"Review*")+COUNTIF(Revenue_Log!T2:T1000,"Review*")+COUNTIF(Voting_Log!P2:P1000,"Review*")'}],
        ],
        "Charity_Report": [
            ["Charity Settlement Report"],
            ["Event Name", {"formula": "Event_Setup!B2"}],
            ["Event Date", {"formula": "Event_Setup!B3"}],
            ["Charity", {"formula": "Event_Setup!B4"}],
            ["Gross Event Revenue", {"formula": "Settlement_Calc!B2"}],
            ["Verified Event Revenue", {"formula": "Settlement_Calc!B3"}],
            ["Net Event Revenue", {"formula": "Settlement_Calc!B5"}],
            ["Charity Share of Net Event Revenue", {"formula": "Settlement_Calc!B9"}],
            ["Net Voting Donations to Charity", {"formula": "Settlement_Calc!B14"}],
            ["FINAL TOTAL TO CHARITY", {"formula": "Settlement_Calc!B15"}],
            ["Settlement Paid?", ""],
            ["Date Paid", ""],
            ["Payment / Check / Transfer ID", ""],
        ],
        "Audit_Flags": [["Issue"]] + [[flag] for flag in [r["audit"] for r in records if r["audit"]]],
    }
    return _xlsx_bytes(sheets)
