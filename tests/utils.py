"""
Copyright (C) 2022 Matthew Hendrey

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

from datetime import datetime
import numpy as np
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytz
from string import hexdigits
from typing import Union


def yield_tables(
    n_files: int = 10,
    n_records_per_file: int = 100,
    use_date: bool = False,
    timezone: str = None,
):
    n_data = n_records_per_file * n_files
    rng = np.random.default_rng(812)
    if timezone:
        tz = pytz.timezone(timezone)
    else:
        tz = None

    pa_fields = [
        ("id", pa.string()),
        ("timestamp", pa.timestamp("us", tz=tz)),
        ("x", pa.int32()),
    ]
    if use_date:
        pa_fields[1] = ("timestamp", pa.date32())
    pa_schema = pa.schema(pa_fields)

    ids = sorted("".join(s) for s in rng.choice(list(hexdigits[:16]), (n_data, 2)))
    x = rng.integers(1, 500, n_data)

    starting_epoch = 1626365594  # 2021-07-15T12:13:14Z
    delta = 5356800  # Roughly a 2-month delta
    for i in range(n_files):
        epoch_min = starting_epoch + i * delta
        epoch_max = epoch_min + int(delta // 2)
        timestamps = [datetime.fromtimestamp(epoch_min, tz=tz)]
        for e in rng.integers(
            epoch_min + (60 * 60 * 24),
            epoch_max - (60 * 60 * 24),
            n_records_per_file - 2,
        ):
            timestamps.append(datetime.fromtimestamp(e, tz=tz))
        timestamps.append(datetime.fromtimestamp(epoch_max, tz=tz))
        if use_date:
            timestamps = [ts.date() for ts in timestamps]

        yield pa.Table.from_pydict(
            {
                "id": ids[i * n_records_per_file : (i + 1) * n_records_per_file],
                "timestamp": timestamps,
                "x": x[i * n_records_per_file : (i + 1) * n_records_per_file],
            },
            schema=pa_schema,
        )


def write_parquet_files(
    output_dir: Union[str, Path],
    n_files: int = 10,
    n_records_per_file: int = 100,
    row_group_size: int = 20,
    use_date: bool = False,
    timezone: str = None,
):
    """Generate some parquet files for testing purposes

    Parameters
    ----------
    output_dir : Union[str, Path]
        Output directory to write parquet files to
    n_files : int, optional
        Number of parquet files to write, by default 10
    n_records_per_file : int, optional
        Number of records per file, by default 100
    row_group_size : int, optional
        Number of row groups to have per file, by default 20
    use_date : bool, optional
        If True make the timestamp column dates, by default False (datetime)
    timezone : str, optional
        Timezone to use for timestamp column, by default None (naive)
    """
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    basename = f"{output_dir}/part"
    for idx, table in enumerate(
        yield_tables(n_files, n_records_per_file, use_date, timezone)
    ):
        min_id = pc.min(table.column(0))
        max_id = pc.max(table.column(0))
        min_ts = pc.min(table.column(1)).cast(pa.date32())
        max_ts = pc.max(table.column(1)).cast(pa.date32())
        pq.write_table(
            table,
            f"{basename}-{idx:02}-{min_id}_{max_id}_{min_ts}_{max_ts}.gzip.parquet",
            row_group_size=row_group_size,
            version="2.6",
            compression="gzip",
        )
