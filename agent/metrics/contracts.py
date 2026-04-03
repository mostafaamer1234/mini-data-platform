from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(slots=True)
class MetricContract:
    contract_id: str
    sql: str
    expected_columns: list[str] = field(default_factory=list)
    expected_grain: str = "unknown"
    assumptions: list[str] = field(default_factory=list)


def detect_metric_contract(question: str) -> MetricContract | None:
    lower = question.lower().replace("-", " ")

    if _is_copurchase_question(lower):
        specific_pair = _extract_specific_product_pair(question)
        if specific_pair:
            product_a, product_b = specific_pair
            sql = f"""
            WITH dedup AS (
                SELECT DISTINCT transaction_id, product_name
                FROM marts.fct_orders
                WHERE transaction_id IS NOT NULL AND product_name IS NOT NULL
            ),
            pairs AS (
                SELECT
                    LEAST(a.product_name, b.product_name) AS product_a,
                    GREATEST(a.product_name, b.product_name) AS product_b,
                    a.transaction_id
                FROM dedup a
                JOIN dedup b
                  ON a.transaction_id = b.transaction_id
                 AND a.product_name < b.product_name
            )
            SELECT
                LEAST('{_escape_sql_literal(product_a)}', '{_escape_sql_literal(product_b)}') AS product_a,
                GREATEST('{_escape_sql_literal(product_a)}', '{_escape_sql_literal(product_b)}') AS product_b,
                COUNT(DISTINCT transaction_id) AS pair_count
            FROM pairs
            WHERE product_a = LEAST('{_escape_sql_literal(product_a)}', '{_escape_sql_literal(product_b)}')
              AND product_b = GREATEST('{_escape_sql_literal(product_a)}', '{_escape_sql_literal(product_b)}')
            """
            return MetricContract(
                contract_id="copurchase_specific_pair",
                sql=sql,
                expected_columns=["product_a", "product_b", "pair_count"],
                expected_grain="product_pair",
                assumptions=["pair_count is COUNT(DISTINCT transaction_id) over deduplicated product pairs"],
            )

        sql = """
        WITH dedup AS (
            SELECT DISTINCT transaction_id, product_name
            FROM marts.fct_orders
            WHERE transaction_id IS NOT NULL AND product_name IS NOT NULL
        ),
        pairs AS (
            SELECT
                LEAST(a.product_name, b.product_name) AS product_a,
                GREATEST(a.product_name, b.product_name) AS product_b,
                a.transaction_id
            FROM dedup a
            JOIN dedup b
              ON a.transaction_id = b.transaction_id
             AND a.product_name < b.product_name
        )
        SELECT
            product_a,
            product_b,
            COUNT(DISTINCT transaction_id) AS pair_count
        FROM pairs
        GROUP BY 1, 2
        ORDER BY pair_count DESC, product_a, product_b
        LIMIT 1
        """
        return MetricContract(
            contract_id="copurchase_top_pair",
            sql=sql,
            expected_columns=["product_a", "product_b", "pair_count"],
            expected_grain="product_pair",
            assumptions=["pair_count is COUNT(DISTINCT transaction_id) over deduplicated product pairs"],
        )

    if _is_clv_average_question(lower):
        sql = """
        WITH per_user AS (
            SELECT user_id, SUM(total) AS clv
            FROM marts.fct_orders
            WHERE user_id IS NOT NULL
            GROUP BY 1
        )
        SELECT AVG(clv) AS average_customer_lifetime_value
        FROM per_user
        """
        return MetricContract(
            contract_id="clv_average",
            sql=sql,
            expected_columns=["average_customer_lifetime_value"],
            expected_grain="overall",
            assumptions=["CLV is defined as SUM(total) per user over all available history"],
        )

    if _is_aov_distinct_txn_method_question(lower):
        year = _extract_year(question)
        if year is None:
            year_filter = (
                "EXTRACT(year FROM transaction_date) = "
                "(SELECT MAX(EXTRACT(year FROM transaction_date)) FROM marts.fct_orders)"
            )
        else:
            year_filter = f"EXTRACT(year FROM transaction_date) = {year}"
        sql = f"""
        WITH metric AS (
            SELECT
                payment_method,
                SUM(total) / NULLIF(COUNT(DISTINCT transaction_id), 0) AS aov
            FROM marts.fct_orders
            WHERE {year_filter}
            GROUP BY 1
        )
        SELECT payment_method, aov
        FROM metric
        ORDER BY aov DESC, payment_method
        LIMIT 1
        """
        return MetricContract(
            contract_id="aov_distinct_txn_top_method",
            sql=sql,
            expected_columns=["payment_method", "aov"],
            expected_grain="payment_method",
            assumptions=["AOV is SUM(total)/COUNT(DISTINCT transaction_id)"],
        )

    if _is_repeat_purchase_event_level_question(lower):
        sql = """
        WITH ordered AS (
            SELECT
                user_id,
                customer_segment,
                transaction_date,
                LAG(transaction_date) OVER (
                    PARTITION BY user_id
                    ORDER BY transaction_date
                ) AS prev_transaction_date
            FROM marts.fct_orders
            WHERE user_id IS NOT NULL
        ),
        gaps AS (
            SELECT
                customer_segment,
                EXTRACT(year FROM transaction_date) AS order_year,
                DATE_DIFF('day', prev_transaction_date, transaction_date) AS days_between_orders
            FROM ordered
            WHERE prev_transaction_date IS NOT NULL
        ),
        segment_year AS (
            SELECT
                customer_segment,
                order_year,
                MEDIAN(days_between_orders) AS median_days_between_orders,
                COUNT(*) AS repeat_order_gaps
            FROM gaps
            GROUP BY 1, 2
        ),
        latest AS (
            SELECT MAX(order_year) AS latest_year
            FROM segment_year
        )
        SELECT
            customer_segment,
            order_year,
            median_days_between_orders,
            repeat_order_gaps
        FROM segment_year
        WHERE order_year = (SELECT latest_year FROM latest)
        ORDER BY customer_segment
        """
        return MetricContract(
            contract_id="repeat_purchase_event_level_latest_year",
            sql=sql,
            expected_columns=[
                "customer_segment",
                "order_year",
                "median_days_between_orders",
                "repeat_order_gaps",
            ],
            expected_grain="customer_segment_for_latest_year",
            assumptions=["Repeat purchase uses raw consecutive order events (no date dedup)"],
        )

    if _is_repeat_purchase_distinct_dates_question(lower):
        sql = """
        WITH daily AS (
            SELECT DISTINCT user_id, customer_segment, CAST(transaction_date AS DATE) AS purchase_day
            FROM marts.fct_orders
            WHERE user_id IS NOT NULL
        ),
        ordered AS (
            SELECT
                user_id,
                customer_segment,
                purchase_day,
                LAG(purchase_day) OVER (
                    PARTITION BY user_id
                    ORDER BY purchase_day
                ) AS prev_purchase_day
            FROM daily
        ),
        gaps AS (
            SELECT
                customer_segment,
                EXTRACT(year FROM purchase_day) AS order_year,
                DATE_DIFF('day', prev_purchase_day, purchase_day) AS days_between_orders
            FROM ordered
            WHERE prev_purchase_day IS NOT NULL
        ),
        segment_year AS (
            SELECT
                customer_segment,
                order_year,
                MEDIAN(days_between_orders) AS median_days_between_orders,
                COUNT(*) AS repeat_order_gaps
            FROM gaps
            GROUP BY 1, 2
        ),
        with_prev AS (
            SELECT
                customer_segment,
                order_year,
                median_days_between_orders,
                LAG(median_days_between_orders) OVER (
                    PARTITION BY customer_segment
                    ORDER BY order_year
                ) AS prior_year_median_days,
                repeat_order_gaps
            FROM segment_year
        ),
        latest AS (
            SELECT MAX(order_year) AS latest_year
            FROM with_prev
        )
        SELECT
            customer_segment,
            order_year,
            median_days_between_orders,
            prior_year_median_days,
            median_days_between_orders - prior_year_median_days AS yoy_absolute_delta_days,
            CASE
                WHEN prior_year_median_days IS NULL OR prior_year_median_days = 0 THEN NULL
                ELSE (median_days_between_orders - prior_year_median_days) / prior_year_median_days
            END AS yoy_percentage_delta,
            repeat_order_gaps
        FROM with_prev
        WHERE order_year = (SELECT latest_year FROM latest)
        ORDER BY customer_segment
        """
        return MetricContract(
            contract_id="repeat_purchase_distinct_dates_latest_year",
            sql=sql,
            expected_columns=[
                "customer_segment",
                "order_year",
                "median_days_between_orders",
                "prior_year_median_days",
                "yoy_absolute_delta_days",
                "yoy_percentage_delta",
                "repeat_order_gaps",
            ],
            expected_grain="customer_segment_for_latest_year",
            assumptions=["Repeat purchase uses DISTINCT user/day events before gap calculation"],
        )

    if _is_qoq_positive_negative_contributor_question(lower):
        sql = """
        WITH latest_q AS (
            SELECT date_trunc('quarter', MAX(transaction_date)) AS quarter_start
            FROM marts.fct_orders
        ),
        qoq AS (
            SELECT
                date_trunc('quarter', transaction_date) AS quarter_start,
                customer_segment,
                category,
                SUM(total) AS revenue
            FROM marts.fct_orders
            GROUP BY 1, 2, 3
        ),
        with_prev AS (
            SELECT
                quarter_start,
                customer_segment,
                category,
                revenue,
                LAG(revenue) OVER (
                    PARTITION BY customer_segment, category
                    ORDER BY quarter_start
                ) AS prior_quarter_revenue
            FROM qoq
        )
        SELECT
            quarter_start,
            customer_segment,
            category,
            revenue,
            prior_quarter_revenue,
            revenue - prior_quarter_revenue AS absolute_delta,
            CASE
                WHEN prior_quarter_revenue IS NULL OR prior_quarter_revenue = 0 THEN NULL
                ELSE (revenue - prior_quarter_revenue) / prior_quarter_revenue
            END AS percentage_delta,
            CASE
                WHEN revenue - prior_quarter_revenue > 0 THEN 'positive'
                WHEN revenue - prior_quarter_revenue < 0 THEN 'negative'
                ELSE 'flat'
            END AS contribution_direction
        FROM with_prev
        WHERE quarter_start = (SELECT quarter_start FROM latest_q)
          AND prior_quarter_revenue IS NOT NULL
        ORDER BY absolute_delta DESC, customer_segment, category
        """
        return MetricContract(
            contract_id="qoq_segment_category_latest_quarter",
            sql=sql,
            expected_columns=[
                "quarter_start",
                "customer_segment",
                "category",
                "revenue",
                "prior_quarter_revenue",
                "absolute_delta",
                "percentage_delta",
                "contribution_direction",
            ],
            expected_grain="segment_category_for_latest_quarter",
            assumptions=["Top and bottom contributors are computed over full latest-quarter result set"],
        )

    return None


def _extract_year(question: str) -> int | None:
    match = re.search(r"\b(20\d{2})\b", question)
    if not match:
        return None
    return int(match.group(1))


def _extract_specific_product_pair(question: str) -> tuple[str, str] | None:
    pattern = re.compile(
        r"for\s+([A-Za-z0-9&'\-\s]+?)\s+and\s+([A-Za-z0-9&'\-\s]+?)(?:[?.!,]|$)",
        re.IGNORECASE,
    )
    match = pattern.search(question.strip())
    if not match:
        return None
    left = match.group(1).strip()
    right = match.group(2).strip()
    if not left or not right:
        return None
    return left, right


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _is_copurchase_question(lower_question: str) -> bool:
    return (
        "bought together" in lower_question
        or "frequently bought" in lower_question
        or "co-purchase" in lower_question
        or "co purchase" in lower_question
    )


def _is_clv_average_question(lower_question: str) -> bool:
    return (
        "customer lifetime value" in lower_question
        or ("average clv" in lower_question)
        or ("average" in lower_question and "clv" in lower_question)
    )


def _is_aov_distinct_txn_method_question(lower_question: str) -> bool:
    return (
        "aov" in lower_question
        and "payment_method" in lower_question
        and ("distinct transaction" in lower_question or "count(distinct transaction_id)" in lower_question)
    )


def _is_repeat_purchase_distinct_dates_question(lower_question: str) -> bool:
    return (
        ("median" in lower_question or "gap" in lower_question)
        and (
            "distinct purchase dates" in lower_question
            or "distinct purchase date" in lower_question
            or "dedup" in lower_question
        )
    )


def _is_repeat_purchase_event_level_question(lower_question: str) -> bool:
    return (
        ("raw consecutive order events" in lower_question or "event level" in lower_question)
        and ("median" in lower_question or "gap" in lower_question)
    )


def _is_qoq_positive_negative_contributor_question(lower_question: str) -> bool:
    return (
        (
            "quarter-over-quarter" in lower_question
            or "quarter over quarter" in lower_question
            or "qoq" in lower_question
        )
        and "biggest positive" in lower_question
        and "negative" in lower_question
        and "contributor" in lower_question
    )
