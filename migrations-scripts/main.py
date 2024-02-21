import psycopg2
import pandas as pd
from utils import securities, positions, pnl
from utils import create_securities_table, create_positions_table, create_pnl_table
from utils import write_row_to_securities, write_row_to_positions, write_row_to_pnl
from utils import to_security, to_position, to_pnl
from constants import SECURITY_WRITE_ENABLE, POSITIONS_WRITE_ENABLE, PNL_WRITE_ENABLE
from constants import HOST, DATABASE, USER, PASSWORD, PORT

securities: pd.DataFrame
positions: pd.DataFrame
pnl: pd.DataFrame

try:
    with psycopg2.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT
    ) as connection:
        with connection.cursor() as cursor:
            create_securities_table(cursor)
            connection.commit()
            print("created securities table\n")
            create_positions_table(cursor)
            connection.commit()
            print("created positions table\n")
            create_pnl_table(cursor)
            connection.commit()
            print("created pnl table\n")
            with open("./checkpoint.txt", "r+") as f:
                securities_checkpoint, positions_checkpoint, pnl_checkpoint = list(
                    map(int, f.read().split())
                )
                print(
                    f"securities checkpoint - left off at {securities_checkpoint} index"
                )
                print(
                    f"positions checkpoint - left off at {positions_checkpoint} index"
                )
                print(f"pnl checkpoint - left off at {pnl_checkpoint} index")

                print("Starting to write into [securities]. ~200k rows")
                for index, row in securities.iterrows():
                    if SECURITY_WRITE_ENABLE and index > securities_checkpoint:
                        print(" ✅ Wrote into[securities] | Index: " + str(index))
                        write_row_to_securities(cursor, to_security(row))
                        connection.commit()
                        f.seek(0)
                        f.write(f"{index} {positions_checkpoint} {pnl_checkpoint}")
                        f.truncate()
                print("[Securities] write done.")
                for index, row in positions.iterrows():
                    if POSITIONS_WRITE_ENABLE and index > positions_checkpoint:
                        print("Starting to write into [Positions]. ~2M")
                        write_row_to_positions(cursor, to_position(row))
                        connection.commit()
                        f.seek(0)
                        f.write(f"{index} {positions_checkpoint} {pnl_checkpoint}")
                        f.truncate()
                print("[Positions] write done.")
                
                for index, row in pnl.iterrows():
                    if PNL_WRITE_ENABLE and index > pnl_checkpoint:
                        print("Starting to write into [PnL]. ~1B")
                        write_row_to_pnl(cursor, to_pnl(row))
                        connection.commit()
                        f.seek(0)
                        f.write(f"{index} {positions_checkpoint} {pnl_checkpoint}")
                        f.truncate()
except Exception as e:
    print(e)
