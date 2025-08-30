@dataclass
class PoolConfig:
dsn: str
minconn: int = 1
maxconn: int = 8


class PooledDB:
def __init__(self, cfg: PoolConfig, name: str):
self.name = name
self.cfg = cfg
self.pool = ThreadedConnectionPool(minconn=cfg.minconn, maxconn=cfg.maxconn, dsn=cfg.dsn)
log.info("Created pool '%s' (%s-%s)", name, cfg.minconn, cfg.maxconn)


@contextlib.contextmanager
def get(self):
conn = self.pool.getconn()
try:
yield conn
finally:
self.pool.putconn(conn)


def closeall(self):
self.pool.closeall()
log.info("Closed pool '%s'", self.name)
