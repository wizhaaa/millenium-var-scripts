import pandas as pd
from dataclasses import dataclass
from psycopg2.extensions import cursor as Cursor
from constants import SECURITY_WRITE_ENABLE, POSITIONS_WRITE_ENABLE, PNL_WRITE_ENABLE


@dataclass
class Security:
    securityid: str
    assetclass: str
    securitytype: str
    tradingcountry: str
    tradingcurrency: str
    issuername: str
    ticker: str
    rating: str
    industrygroup: str
    industry: str
    securityname: str
    maturitydate: str
    underlyingname: str
    regionname: str
    issuercountryofrisk: str


@dataclass
class Position:
    securityid: str
    trader: str
    microstrategy: str
    pod: str
    desk: str
    supervisor: str
    quantity: int


@dataclass
class PnL:
    securityid: str
    date: str
    pnl: float


def create_securities_table(cursor: Cursor):
    """
    Creates securities table.
    """
    cursor.execute(
        """
                    create table if not exists securitieswz (
                        securityid varchar primary key,
                        assetclass varchar,
                        securitytype varchar,
                        tradingcountry varchar,
                        tradingcurrency varchar,
                        issuername varchar,
                        ticker varchar,
                        rating varchar,
                        industrygroup varchar,
                        industry varchar,
                        securityname varchar,
                        maturitydate varchar,
                        underlyingname varchar,
                        regionname varchar,
                        issuercountryofrisk varchar
                    );
                   """
    )


def create_positions_table(cursor: Cursor):
    """
    Creates position table.
    """
    cursor.execute(
        """
                    create table if not exists positionswz (
                        securityid varchar,
                        trader varchar,
                        microstrategy varchar,
                        pod varchar,
                        desk varchar,
                        supervisor varchar,
                        quantity int
                    );
                   """
    )


def create_pnl_table(cursor: Cursor):
    """
    Creates pnl table.
    """
    cursor.execute(
        """
                    create table if not exists pnlwz(
                        securityid varchar,
                        date varchar,
                        pnl int
                    );
                   """
    )


def write_row_to_securities(cursor: Cursor, security: Security):
    """
    Writes to securities table. Assumes securities table already exists.
    """
    cursor.execute(
        """
                    insert into securitieswz (
                        securityid,
                        assetclass,
                        securitytype,
                        tradingcountry,
                        tradingcurrency,
                        issuername,
                        ticker,
                        rating,
                        industrygroup,
                        industry,
                        securityname,
                        maturitydate,
                        underlyingname,
                        regionname,
                        issuercountryofrisk
                    ) values (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                   """,
        (
            security.securityid,
            security.assetclass,
            security.securitytype,
            security.tradingcountry,
            security.tradingcurrency,
            security.issuername,
            security.ticker,
            security.rating,
            security.industrygroup,
            security.industry,
            security.securityname,
            security.maturitydate,
            security.underlyingname,
            security.regionname,
            security.issuercountryofrisk,
        ),
    )


def write_row_to_positions(cursor: Cursor, position: Position):
    """
    Writes to positions table. Assumes positions table already exists.
    """
    cursor.execute(
        """
                    insert into positionswz (
                        securityid,
                        trader,
                        microstrategy,
                        pod,
                        desk,
                        supervisor,
                        quantity
                    ) values (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                   """,
        (
            position.securityid,
            position.trader,
            position.microstrategy,
            position.pod,
            position.desk,
            position.supervisor,
            position.quantity,
        ),
    )


def write_row_to_pnl(cursor: Cursor, pnl: PnL):
    """
    Writes to pnl table. Assumes pnl table already exists.
    """
    cursor.execute(
        """
                    insert into pnlwz (
                        securityid,
                        date,
                        pnl
                    ) values (
                        %s,
                        %s,
                        %s
                    )
                   """,
        (
            pnl.securityid,
            pnl.date,
            pnl.pnl,
        ),
    )


def to_security(security_row: pd.Series):
    """
    Convert Series row from securities table to Security obj
    """
    # First element in Series.values is index
    return Security(*security_row.values[:])


def to_position(position_row: pd.Series):
    """
    Convert Series row from positions table to Position obj
    """
    # First element in Series.values is index
    return Position(*position_row.values[:])


def to_pnl(pnl_row: pd.Series):
    """
    Convert Series row from pnl table to PnL obj
    """
    # First element in Series.values is index
    return PnL(*pnl_row.values[:])


if PNL_WRITE_ENABLE:
    chunksize = 10 ** 5
    pnl = pd.read_csv("../simulated_pnl.csv", chunksize=chunksize)
else:
    pnl = None

if POSITIONS_WRITE_ENABLE:
    positions = pd.read_csv("../data/positions.csv")
else:
    positions = None

if SECURITY_WRITE_ENABLE:
    securities = pd.read_csv("../data/securities.csv")
else:
    securities = None
