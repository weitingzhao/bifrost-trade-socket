"""IB Flex Web Service client (HTTPS + XML).

Complements :mod:`src.connector.ib` (TWS / ib_insync). Process-agnostic; used by monitor API,
portfolio fetch services, and scripts.

Fetches:

- Cash Transactions（资金流水，deposits/withdrawals/transfers/dividends 等）
- Trades（成交明细，Flex Trades 报表，用于补全 account_executions）

Used by:

- Performance Phase 0: POST /transactions/fetch → account_transactions（现金流水）
- Executions history: POST /executions/fetch-flex → account_executions/account_execution_commissions（成交）

Config: IB_FLEX_TOKEN and IB_FLEX_QUERY_ID (env) or pass to fetch_cash_transactions()/fetch_trades().
"""

import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

FLEX_SEND_REQUEST_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest"
FLEX_GET_STATEMENT_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement"
USER_AGENT = "Python/3.10"
MAX_GET_STATEMENT_POLLS = 24
POLL_INTERVAL_SEC = 5
MAX_FLEX_DAYS = 366  # IB Flex: date range cannot exceed 366 days when fd/td are used


def _text(elem: Optional[ET.Element], tag: str, default: str = "") -> str:
    if elem is None:
        return default
    child = elem.find(tag)
    if child is not None and child.text:
        return (child.text or "").strip()
    return default


def _normalize_type(flex_type: str, code: str, amount: float = 0) -> str:
    """Map Flex Type/Code to our canonical type: deposit, withdrawal, transfer, dividend, other.
    When type is Deposits/Withdrawals, use amount sign: positive -> deposit, negative -> withdrawal."""
    t = (flex_type or "").strip().lower()
    c = (code or "").strip().upper()
    if "deposits/withdrawals" in t or "deposits and withdrawals" in t:
        return "deposit" if amount >= 0 else "withdrawal"
    if "deposit" in t or "dep" in c or c == "DEP":
        return "deposit"
    if "withdraw" in t or "withdrawal" in t or "wth" in c or c == "WTH":
        return "withdrawal"
    if "transfer" in t or "tra" in c or "internal" in t:
        return "transfer"
    if "dividend" in t or "div" in c or "dvd" in c:
        return "dividend"
    return "other"


def request_report(
    token: str,
    query_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[int] = None,
) -> str:
    """
    Call Flex SendRequest; returns ReferenceCode on success.
    - period: IB period override (e.g. 5 = Last 365 Calendar Days). When set, fd/td are ignored.
    - When both from_date and to_date are set (and period is None), IB allows max 366 days.
    """
    params: Dict[str, str] = {"t": token, "q": query_id, "v": "3"}
    if period is not None:
        params["p"] = str(period)
    elif from_date and to_date:
        try:
            fd = datetime.strptime(from_date.strip()[:8], "%Y%m%d")
            td = datetime.strptime(to_date.strip()[:8], "%Y%m%d")
            if (td - fd).days > MAX_FLEX_DAYS:
                raise ValueError(
                    f"Date range cannot exceed {MAX_FLEX_DAYS} days (IB Flex limit). "
                    "Use from_date and to_date within 366 days, or omit both to use the query default period (e.g. Last 365 Calendar Days)."
                )
        except ValueError as e:
            if "cannot exceed" in str(e):
                raise
            raise ValueError(f"from_date and to_date must be yyyyMMdd format: {e}") from e
        params["fd"] = from_date.strip()[:8]
        params["td"] = to_date.strip()[:8]
    elif from_date or to_date:
        raise ValueError("When overriding dates, both from_date and to_date are required (yyyyMMdd).")
    url = f"{FLEX_SEND_REQUEST_URL}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": USER_AGENT}, method="GET")
    try:
        with urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError) as e:
        logger.warning("Flex SendRequest HTTP error: %s", e)
        raise ValueError(f"Flex SendRequest failed: {e}") from e
    root = ET.fromstring(body)
    status = _text(root, "Status", "").strip().lower()
    if status == "fail":
        err_code = _text(root, "ErrorCode", "")
        err_msg = _text(root, "ErrorMessage", "Unknown error")
        logger.warning("Flex SendRequest Status=Fail [%s] %s", err_code, err_msg)
        raise ValueError(f"Flex request failed: [{err_code}] {err_msg}")
    ref = _text(root, "ReferenceCode", "").strip()
    if not ref:
        raise ValueError("Flex SendRequest returned no ReferenceCode")
    return ref


def get_statement(token: str, reference_code: str) -> str:
    """
    Call Flex GetStatement with the given ReferenceCode.
    Returns raw response body (XML statement). May poll a few times if report not ready.
    """
    params = {"t": token, "q": reference_code, "v": "3"}
    url = f"{FLEX_GET_STATEMENT_URL}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": USER_AGENT}, method="GET")
    last_err: Optional[str] = None
    for attempt in range(MAX_GET_STATEMENT_POLLS):
        try:
            with urlopen(req, timeout=90) as resp:
                body = resp.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError) as e:
            last_err = str(e)
            logger.debug("Flex GetStatement attempt %s: %s", attempt + 1, e)
            time.sleep(POLL_INTERVAL_SEC)
            continue
        # If response looks like error XML (Status Fail), treat as not ready and retry
        if body.strip().startswith("<?xml") or body.strip().startswith("<"):
            try:
                root = ET.fromstring(body)
                if _text(root, "Status", "").strip().lower() == "fail":
                    last_err = _text(root, "ErrorMessage", "Report not ready")
                    logger.debug("Flex GetStatement not ready: %s", last_err)
                    time.sleep(POLL_INTERVAL_SEC)
                    continue
            except ET.ParseError:
                pass
        return body
    raise ValueError(
        f"Flex GetStatement did not return report after {MAX_GET_STATEMENT_POLLS} attempts. Last: {last_err}"
    )


def _parse_yyyymmdd_to_date(s: str) -> Optional[datetime]:
    """Helper: parse YYYYMMDD to date (kept as datetime.date compatible with psycopg)."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:8], "%Y%m%d").date()
    except ValueError:
        return None


def parse_cash_transactions_xml(xml_body: str) -> List[Dict[str, Any]]:
    """
    Parse Flex statement XML and return list of cash transaction dicts.
    Each dict at minimum: account_id, ts (Unix float), amount, type, currency, description.
    Also includes extended fields (when present): flex_type, code, flex_transaction_id, asset_category,
    asset_subcategory, symbol, conid, security_id, security_id_type, listing_exchange, report_date,
    available_for_trading_date, fx_rate_to_base, raw_extra (other Flex attributes).
    Handles both element-based and attribute-based CashTransaction nodes.
    Uses report-level accountId (FlexStatement) when row-level CashTransaction has no accountId.
    """
    def strip_ns(tag: str) -> str:
        if tag and "}" in tag:
            return tag.split("}", 1)[1]
        return tag or ""

    try:
        root = ET.fromstring(xml_body)
    except ET.ParseError as e:
        logger.warning("Flex XML parse error: %s", e)
        return []

    # Report-level accountId: IB Flex often has FlexStatement with accountId (one report = one account).
    report_account_id = ""
    for node in root.iter():
        if strip_ns(node.tag) == "FlexStatement":
            report_account_id = (
                node.get("accountId") or node.get("accountID")
                or _text(node, "accountId") or _text(node, "AccountId")
                or ""
            ).strip()
            if report_account_id:
                break
    if not report_account_id and root.attrib:
        report_account_id = (root.get("accountId") or root.get("accountID") or "").strip()

    out: List[Dict[str, Any]] = []
    for elem in root.iter():
        if strip_ns(elem.tag) != "CashTransaction":
            continue
        account_id = _text(elem, "accountId") or _text(elem, "AccountId")
        date_time_str = _text(elem, "dateTime") or _text(elem, "DateTime") or _text(elem, "settleDate")
        amount_str = _text(elem, "amount") or _text(elem, "Amount")
        flex_type = _text(elem, "type") or _text(elem, "Type")
        code = _text(elem, "code") or _text(elem, "Code")
        currency = _text(elem, "currency") or _text(elem, "Currency")
        description = _text(elem, "description") or _text(elem, "Description")
        transaction_id = _text(elem, "transactionID") or _text(elem, "TransactionID")
        asset_category = _text(elem, "assetCategory") or _text(elem, "AssetCategory")
        sub_category = _text(elem, "subCategory") or _text(elem, "SubCategory")
        symbol = _text(elem, "symbol") or _text(elem, "Symbol")
        conid = _text(elem, "conid") or _text(elem, "Conid")
        security_id = _text(elem, "securityID") or _text(elem, "SecurityID")
        security_id_type = _text(elem, "securityIDType") or _text(elem, "SecurityIDType")
        listing_exchange = _text(elem, "listingExchange") or _text(elem, "ListingExchange")
        report_date = _text(elem, "reportDate") or _text(elem, "ReportDate")
        available_for_trading_date = _text(elem, "availableForTradingDate") or _text(elem, "AvailableForTradingDate")
        fx_rate_to_base = _text(elem, "fxRateToBase") or _text(elem, "FxRateToBase")
        if not account_id:
            account_id = elem.get("accountId") or elem.get("accountID") or ""
        if not account_id:
            account_id = report_account_id
        if not date_time_str:
            date_time_str = elem.get("dateTime") or elem.get("settleDate") or ""
        if not amount_str:
            amount_str = elem.get("amount") or "0"
        if not flex_type:
            flex_type = elem.get("type") or ""
        if not code:
            code = elem.get("code") or ""
        # Many Flex templates put fields only as attributes; fall back to attributes when elements are absent.
        if not currency:
            currency = elem.get("currency") or ""
        if not description:
            description = elem.get("description") or ""
        if not asset_category:
            asset_category = elem.get("assetCategory") or ""
        if not sub_category:
            sub_category = elem.get("subCategory") or ""
        if not symbol:
            symbol = elem.get("symbol") or ""
        if not conid:
            conid = elem.get("conid") or ""
        if not security_id:
            security_id = elem.get("securityID") or ""
        if not security_id_type:
            security_id_type = elem.get("securityIDType") or ""
        if not listing_exchange:
            listing_exchange = elem.get("listingExchange") or ""
        if not report_date:
            report_date = elem.get("reportDate") or ""
        if not available_for_trading_date:
            available_for_trading_date = elem.get("availableForTradingDate") or ""
        if not fx_rate_to_base:
            fx_rate_to_base = elem.get("fxRateToBase") or ""

        try:
            amount = float(amount_str.replace(",", ""))
        except (ValueError, TypeError):
            amount = 0.0
        if not account_id and amount == 0:
            continue
        ts_parsed: Optional[datetime] = None
        s = date_time_str.strip()
        for fmt in ("%Y%m%d;%H%M%S", "%Y%m%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                # Use slice so we don't pass extra chars (e.g. timezone) that break parsing
                n = 19 if "T" in fmt or " " in fmt else 8
                ts_parsed = datetime.strptime(s[:n], fmt)
                if ts_parsed.tzinfo is None:
                    ts_parsed = ts_parsed.replace(tzinfo=timezone.utc)
                break
            except (ValueError, TypeError):
                continue
        if ts_parsed is None and s:
            m = re.search(r"(\d{8})", s)
            if m:
                try:
                    ts_parsed = datetime.strptime(m.group(1), "%Y%m%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
        if ts_parsed is None:
            ts_parsed = datetime.now(timezone.utc)
        ts_float = ts_parsed.timestamp()
        tx_type = _normalize_type(flex_type, code, amount)
        # Collect remaining attributes into raw_extra for future use
        raw_extra = dict(elem.attrib)
        # Normalize some known attribute names to lower_snake for convenience
        for k in list(raw_extra.keys()):
            v = raw_extra[k]
            raw_extra[k] = v

        row: Dict[str, Any] = {
            "account_id": account_id.strip(),
            "ts": ts_float,
            "amount": amount,
            "type": tx_type,
            "currency": currency or None,
            "description": description or None,
        }
        if transaction_id:
            row["flex_transaction_id"] = transaction_id
        if flex_type:
            row["flex_type"] = flex_type
        if code:
            row["flex_code"] = code
        if asset_category:
            row["asset_category"] = asset_category
        if sub_category:
            row["asset_subcategory"] = sub_category
        if symbol:
            row["symbol"] = symbol
        if conid:
            try:
                row["conid"] = int(conid)
            except (TypeError, ValueError):
                row["conid"] = None
        if security_id:
            row["security_id"] = security_id
        if security_id_type:
            row["security_id_type"] = security_id_type
        if listing_exchange:
            row["listing_exchange"] = listing_exchange
        if report_date:
            row["report_date"] = report_date
        if available_for_trading_date:
            row["available_for_trading_date"] = available_for_trading_date
        if fx_rate_to_base:
            try:
                row["fx_rate_to_base"] = float(fx_rate_to_base)
            except (TypeError, ValueError):
                row["fx_rate_to_base"] = None
        if raw_extra:
            row["raw_extra"] = raw_extra
        out.append(row)
    return out


def parse_trades_xml(xml_body: str) -> List[Dict[str, Any]]:
    """
    Parse Flex statement XML Trades section and return list of execution-like dicts.

    每条 dict 形状适配 write_account_executions_to_db / PostgreSQLSink.write_account_executions：
    - 基本字段：account_id, time(Unix s), symbol, sec_type, side, quantity, price, source, exec_id, expiry,
      strike, option_right, exchange, order_id, contract_key, 以及若干 Flex 扩展字段（见 DATABASE.md §2.11）。
    - Commission / realized PnL：填入 commission, currency, realized_pnl，由上层写入 account_execution_commissions。
    其余未单独列出的 Flex 属性打包到 raw_extra。
    """

    def strip_ns(tag: str) -> str:
        if tag and "}" in tag:
            return tag.split("}", 1)[1]
        return tag or ""

    try:
        root = ET.fromstring(xml_body)
    except ET.ParseError as e:
        logger.warning("Flex Trades XML parse error: %s", e)
        return []

    # Report-level accountId fallback
    report_account_id = ""
    for node in root.iter():
        if strip_ns(node.tag) == "FlexStatement":
            report_account_id = (
                node.get("accountId")
                or node.get("accountID")
                or _text(node, "accountId")
                or _text(node, "AccountId")
                or ""
            ).strip()
            if report_account_id:
                break
    if not report_account_id and root.attrib:
        report_account_id = (root.get("accountId") or root.get("accountID") or "").strip()

    out: List[Dict[str, Any]] = []
    for elem in root.iter():
        if strip_ns(elem.tag) != "Trade":
            continue
        attrs = {k: (v or "").strip() for k, v in elem.attrib.items()}

        account_id = attrs.get("accountId") or report_account_id
        if not account_id:
            account_id = ""
        # 时间：Flex Trades 通常为 YYYYMMDD;HHMMSS
        date_time_str = attrs.get("dateTime", "")
        ts_parsed: Optional[datetime] = None
        s = date_time_str.strip()
        for fmt in ("%Y%m%d;%H%M%S", "%Y%m%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                n = 19 if "T" in fmt or " " in fmt else 8
                ts_parsed = datetime.strptime(s[:n], fmt)
                if ts_parsed.tzinfo is None:
                    ts_parsed = ts_parsed.replace(tzinfo=timezone.utc)
                break
            except (ValueError, TypeError):
                continue
        if ts_parsed is None and s:
            m = re.search(r"(\d{8})", s)
            if m:
                try:
                    ts_parsed = datetime.strptime(m.group(1), "%Y%m%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    ts_parsed = None
        if ts_parsed is None:
            # 再退一步：使用 tradeDate 作为日期
            td = attrs.get("tradeDate") or ""
            td_dt = None
            try:
                if td:
                    td_dt = datetime.strptime(td[:8], "%Y%m%d").replace(tzinfo=timezone.utc)
            except ValueError:
                td_dt = None
            ts_parsed = td_dt
        if ts_parsed is None:
            # 没有时间且没有账户/数量信息就丢弃
            if not account_id:
                continue
        time_unix = ts_parsed.timestamp() if ts_parsed is not None else None

        # 基本 Execution 字段
        asset_category = attrs.get("assetCategory", "")
        sec_type = asset_category or ""
        side = attrs.get("buySell", "").upper() or ""
        symbol = attrs.get("symbol", "")
        quantity = attrs.get("quantity", "0")
        price = attrs.get("tradePrice", "0")
        expiry = attrs.get("expiry", "")
        strike = attrs.get("strike", "")
        option_right = attrs.get("putCall", "")
        exchange = attrs.get("exchange", "") or attrs.get("listingExchange", "")
        order_id = attrs.get("ibOrderID", "") or attrs.get("orderID", "")
        exec_id = attrs.get("ibExecID", "")

        try:
            qty_val: Optional[float] = float(quantity.replace(",", "")) if quantity else None
        except (ValueError, TypeError):
            qty_val = None
        try:
            price_val: Optional[float] = float(price.replace(",", "")) if price else None
        except (ValueError, TypeError):
            price_val = None

        # contract_key：symbol|sec_type|expiry|strike|right
        contract_key = None
        sym_key = (symbol or "").strip()
        sec_key = (sec_type or "").strip()
        exp_key = (expiry or "").strip()
        try:
            strike_key = float(strike) if strike not in ("", None) else None
        except (ValueError, TypeError):
            strike_key = None
        right_key = (option_right or "").strip()
        if sym_key and sec_key:
            contract_key = "|".join(
                [
                    sym_key,
                    sec_key,
                    exp_key,
                    str(strike_key) if strike_key is not None else "",
                    right_key,
                ]
            )

        # Flex 扩展字段
        report_date = _parse_yyyymmdd_to_date(attrs.get("reportDate", ""))
        trade_date = _parse_yyyymmdd_to_date(attrs.get("tradeDate", ""))
        settle_date_target = _parse_yyyymmdd_to_date(attrs.get("settleDateTarget", ""))

        def _f(name: str) -> Optional[float]:
            v = attrs.get(name, "")
            if not v:
                return None
            try:
                return float(v.replace(",", ""))
            except (ValueError, TypeError):
                return None

        proceeds = _f("proceeds")
        taxes = _f("taxes")
        net_cash = _f("netCash")
        close_price = _f("closePrice")
        cost = _f("cost")
        fifo_pnl_realized = _f("fifoPnlRealized")
        mtm_pnl = _f("mtmPnl")
        trade_money = _f("tradeMoney")
        fx_rate_to_base = _f("fxRateToBase")
        multiplier = _f("multiplier")

        # Commission / realized PnL（由上层写入 account_execution_commissions）
        commission = _f("ibCommission")
        commission_currency = attrs.get("ibCommissionCurrency", "") or ""

        row: Dict[str, Any] = {
            "account_id": account_id,
            "time": time_unix,
            "symbol": symbol or None,
            "sec_type": sec_type or None,
            "side": side or None,
            "quantity": qty_val,
            "price": price_val,
            "source": "flex_trades",
            "exec_id": exec_id or None,
            "expiry": expiry or None,
            "strike": strike_key,
            "option_right": option_right or None,
            "exchange": exchange or None,
            "order_id": int(order_id) if order_id not in ("", None) and order_id.isdigit() else None,
            "cum_qty": None,
            "contract_key": contract_key,
            "currency": attrs.get("currency") or None,
            "asset_category": asset_category or None,
            "sub_category": attrs.get("subCategory") or None,
            "description": attrs.get("description") or None,
            "conid": int(attrs.get("conid", "0")) if attrs.get("conid", "").isdigit() else None,
            "security_id": attrs.get("securityID") or None,
            "security_id_type": attrs.get("securityIDType") or None,
            "cusip": attrs.get("cusip") or None,
            "isin": attrs.get("isin") or None,
            "figi": attrs.get("figi") or None,
            "listing_exchange": attrs.get("listingExchange") or None,
            "underlying_conid": int(attrs.get("underlyingConid", "0")) if attrs.get("underlyingConid", "").isdigit() else None,
            "underlying_symbol": attrs.get("underlyingSymbol") or None,
            "underlying_security_id": attrs.get("underlyingSecurityID") or None,
            "underlying_listing_exchange": attrs.get("underlyingListingExchange") or None,
            "issuer": attrs.get("issuer") or None,
            "issuer_country_code": attrs.get("issuerCountryCode") or None,
            "trade_id": attrs.get("tradeID") or None,
            "related_trade_id": attrs.get("relatedTradeID") or None,
            "report_date": report_date,
            "trade_date": trade_date,
            "settle_date_target": settle_date_target,
            "transaction_type": attrs.get("transactionType") or None,
            "multiplier": multiplier,
            "principal_adjust_factor": attrs.get("principalAdjustFactor") or None,
            "proceeds": proceeds,
            "taxes": taxes,
            "net_cash": net_cash,
            "close_price": close_price,
            "open_close_indicator": attrs.get("openCloseIndicator") or None,
            "notes": attrs.get("notes") or None,
            "cost": cost,
            "fifo_pnl_realized": fifo_pnl_realized,
            "mtm_pnl": mtm_pnl,
            "trade_money": trade_money,
            "fx_rate_to_base": fx_rate_to_base,
            "acct_alias": attrs.get("acctAlias") or None,
            "model": attrs.get("model") or None,
            "commission": commission,
            "currency_commission": commission_currency or None,
            "realized_pnl": fifo_pnl_realized,
        }

        # 其余属性打包进 raw_extra，避免丢信息
        known_keys = {
            "accountId",
            "acctAlias",
            "model",
            "currency",
            "fxRateToBase",
            "assetCategory",
            "subCategory",
            "symbol",
            "description",
            "conid",
            "securityID",
            "securityIDType",
            "cusip",
            "isin",
            "figi",
            "listingExchange",
            "underlyingConid",
            "underlyingSymbol",
            "underlyingSecurityID",
            "underlyingListingExchange",
            "issuer",
            "issuerCountryCode",
            "tradeID",
            "multiplier",
            "relatedTradeID",
            "strike",
            "reportDate",
            "expiry",
            "dateTime",
            "putCall",
            "tradeDate",
            "principalAdjustFactor",
            "settleDateTarget",
            "transactionType",
            "exchange",
            "quantity",
            "tradePrice",
            "tradeMoney",
            "proceeds",
            "taxes",
            "ibCommission",
            "ibCommissionCurrency",
            "netCash",
            "closePrice",
            "openCloseIndicator",
            "notes",
            "cost",
            "fifoPnlRealized",
            "mtmPnl",
        }
        extra: Dict[str, Any] = {}
        for k, v in attrs.items():
            if k not in known_keys:
                extra[k] = v
        if extra:
            row["raw_extra"] = extra

        # 若同时 currency_commission 与 currency 冲突，以 commission 的币种为准
        if row.get("currency_commission"):
            row["currency"] = row.get("currency") or row.get("currency_commission")
        # 兼容 write_account_executions_to_db 的字段名
        if row.get("currency_commission"):
            row["currency"] = row["currency"]

        out.append(row)
    # 若解析到 0 条且响应里包含 CashTransaction，说明用的很可能是 Cash Transactions 的 Query ID，而不是 Trades
    if len(out) == 0 and "CashTransaction" in xml_body:
        raise ValueError(
            "Flex report contains Cash Transactions, not Trades. "
            "Use a Flex Query that includes Activity → Trades (not Cash Transactions). "
            "In Settings → IB Connection → Flex, ensure the row with purpose=trades uses a Query ID that returns Trades."
        )
    return out


def fetch_cash_transactions(
    token: str,
    query_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Request Flex report and parse Cash Transactions.
    from_date / to_date: yyyyMMdd. When both are omitted, do not override dates and let the Flex query's default period apply.
    Returns list of dicts with account_id, ts, amount, type, currency, description.
    """
    if from_date is None and to_date is None:
        # Do not set fd/td; rely on Flex query configuration (e.g. Last 365 Calendar Days).
        pass
    ref = request_report(token, query_id, from_date, to_date)
    logger.info("Flex SendRequest OK, reference=%s", ref)
    body = get_statement(token, ref)
    return parse_cash_transactions_xml(body)


def fetch_trades(
    token: str,
    query_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Request Flex report and parse Trades section into execution dicts for account_executions.

    from_date / to_date: yyyyMMdd. When both are None and period is None, no fd/td or p is sent (query default).
    period: e.g. 5 = Last 365 Calendar Days. When set, overrides fd/td (same as script --last-365).
    """
    ref = request_report(token, query_id, from_date=from_date, to_date=to_date, period=period)
    logger.info("Flex Trades SendRequest OK, reference=%s", ref)
    body = get_statement(token, ref)
    return parse_trades_xml(body)
