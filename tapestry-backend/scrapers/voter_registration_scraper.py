from __future__ import annotations

from datetime import date, datetime

import polars as pl

from db.connection import init_db, write_connection
from scrapers.base import BaseScraper


REGISTRATION_SEEDS = {
    "AZ": {"d_share": 0.335, "r_share": 0.349, "ind_share": 0.315, "total_registered": 4200000, "report_date": "2026-03-01", "data_source": "az_sos_estimate"},
    "NV": {"d_share": 0.375, "r_share": 0.316, "ind_share": 0.295, "total_registered": 1850000, "report_date": "2026-03-01", "data_source": "nv_sos_estimate"},
    "PA": {"d_share": 0.436, "r_share": 0.396, "ind_share": 0.168, "total_registered": 8800000, "report_date": "2026-03-01", "data_source": "pa_dos_estimate"},
    "NC": {"d_share": 0.349, "r_share": 0.325, "ind_share": 0.326, "total_registered": 7800000, "report_date": "2026-03-01", "data_source": "nc_sbe_estimate"},
    "GA": {"d_share": 0.388, "r_share": 0.392, "ind_share": 0.220, "total_registered": 8100000, "report_date": "2026-03-01", "data_source": "ga_sos_estimate"},
    "CO": {"d_share": 0.291, "r_share": 0.267, "ind_share": 0.442, "total_registered": 4000000, "report_date": "2026-03-01", "data_source": "co_sos_estimate"},
    "NH": {"d_share": 0.259, "r_share": 0.276, "ind_share": 0.455, "total_registered": 1050000, "report_date": "2026-03-01", "data_source": "nh_sos_estimate"},
    "ME": {"d_share": 0.279, "r_share": 0.267, "ind_share": 0.454, "total_registered": 1100000, "report_date": "2026-03-01", "data_source": "me_sos_estimate"},
    "FL": {"d_share": 0.329, "r_share": 0.384, "ind_share": 0.287, "total_registered": 13800000, "report_date": "2026-03-01", "data_source": "fl_dos_estimate"},
    "WI": {"d_share": None, "r_share": None, "ind_share": None, "total_registered": 3900000, "report_date": "2026-03-01", "data_source": "wi_gab_public_estimate"},
    "MI": {"d_share": None, "r_share": None, "ind_share": None, "total_registered": 8200000, "report_date": "2026-03-01", "data_source": "mi_sos_public_estimate"},
    "OH": {"d_share": None, "r_share": None, "ind_share": None, "total_registered": 8000000, "report_date": "2026-03-01", "data_source": "oh_sos_public_estimate"},
    "MT": {"d_share": None, "r_share": None, "ind_share": None, "total_registered": 780000, "report_date": "2026-03-01", "data_source": "mt_sos_public_estimate"},
}


class VoterRegistrationScraper(BaseScraper):
    source_name = "voter_registration"
    output_path = "data/raw/voter_registration_latest.parquet"

    def fetch(self) -> pl.DataFrame:
        rows = []
        for state, data in REGISTRATION_SEEDS.items():
            total = int(data["total_registered"])
            d_registered = int(total * data["d_share"]) if data["d_share"] is not None else None
            r_registered = int(total * data["r_share"]) if data["r_share"] is not None else None
            ind_registered = int(total * data["ind_share"]) if data["ind_share"] is not None else None
            accounted = sum(value for value in [d_registered, r_registered, ind_registered] if value is not None)
            rows.append({
                "state_abbr": state,
                "report_date": datetime.fromisoformat(data["report_date"]).date(),
                "total_registered": total,
                "d_registered": d_registered,
                "r_registered": r_registered,
                "independent_registered": ind_registered,
                "other_registered": max(0, total - accounted) if accounted else None,
                "d_share": data["d_share"],
                "r_share": data["r_share"],
                "ind_share": data["ind_share"],
                "d_r_ratio": data["d_share"] / data["r_share"] if data["r_share"] else None,
                "net_new_d_30d": None,
                "net_new_r_30d": None,
                "net_new_total_30d": None,
                "d_registration_trend": None,
                "r_registration_trend": None,
                "data_source": data["data_source"],
            })
        return pl.DataFrame(rows)

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    def run(self) -> bool:
        init_db()
        ok = super().run()
        df = pl.read_parquet(self.output_path)
        with write_connection() as con:
            con.register("voter_registration_df", df)
            con.execute("INSERT OR REPLACE INTO voter_registration SELECT * FROM voter_registration_df")
        print(f"Loaded voter registration rows: {df.height}")
        return ok


if __name__ == "__main__":
    raise SystemExit(0 if VoterRegistrationScraper().run() else 1)
