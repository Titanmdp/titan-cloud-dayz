"""
livonia_elevation.py
====================
Módulo otimizado para query de elevação no Livonia (Enoch) — DayZ Standalone
para uso em sistemas de loja / economia de servidor.

Parâmetros reais do terrain (Enoch / Arma 3 Contact port):
  - Grid: 4096×4096 pontos
  - Cell size: 3.125 metros
  - Mapa: 12.800×12.800 metros (~163 km²)
  - Coordenada X: leste-oeste (0=oeste)
  - Coordenada Z: sul-norte   (0=sul/área de spawn)
  - Norte: Topolin, Grabin (planícies)
  - Sul: Dambog, Sowa, bases militares (montanhoso/florestal)

Uso rápido:
    from livonia_elevation import LivoniaHeightmap
    hm = LivoniaHeightmap()
    elev = hm.get_elevation(1300.0, 1800.0)   # Dambog (pico SW)
    zone  = hm.classify_zone(6200.0, 11200.0)  # Topolin (norte)
    price = compute_shop_price(1000, 7200.0, 3800.0, hm)  # Kopa Prison

Carregar dados reais (recomendado):
    hm = LivoniaHeightmap.from_asc("enoch_heightmap.asc")
    hm = LivoniaHeightmap.from_npy("livonia_heightmap.npy")
"""

from __future__ import annotations
import numpy as np
import json, os, time
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any


# ─── Constantes do Terrain ────────────────────────────────────────────────────

GRID_SIZE  = 4096
CELL_SIZE  = 3.125          # metros — 12800/4096
MAP_SIZE   = GRID_SIZE * CELL_SIZE   # 12800.0 m
XLLCORNER  = 200_000.0
YLLCORNER  = 0.0
MAP_NAME   = "Livonia"
INTERNAL   = "Enoch"


# ─── Zonas de elevação específicas do Livonia ─────────────────────────────────
# Livonia é MENOS extremo que Chernarus — teto ~385m vs ~730m
# Mas o sul militar/florestal é muito mais hostil → multiplicador maior

ELEVATION_ZONES: Dict[str, Dict] = {
    "planicie":   {"range": (  0,  80), "tier": 1, "multiplier": 1.00,
                   "description": "Planície e vales — bordas norte/sul"},
    "campos":     {"range": ( 80, 160), "tier": 1, "multiplier": 1.05,
                   "description": "Campos e florestas abertas"},
    "colinas":    {"range": (160, 230), "tier": 2, "multiplier": 1.15,
                   "description": "Colinas e florestas densas — maioria do norte"},
    "montanha":   {"range": (230, 300), "tier": 3, "multiplier": 1.30,
                   "description": "Região montanhosa — sul militar (Nadbor, SILA)"},
    "alto":       {"range": (300, 360), "tier": 4, "multiplier": 1.50,
                   "description": "Alta altitude — Swarog, Krsnik, Rodzanica"},
    "pico":       {"range": (360, 500), "tier": 5, "multiplier": 1.75,
                   "description": "Picos extremos — Dambog Bunker, Sowa"},
}

# Pontos conhecidos (X, Z em metros in-game)
KNOWN_POINTS: Dict[str, Tuple[float, float]] = {
    # Cidades/Vilas
    "topolin":         (6200, 11200),   # capital, norte
    "grabin":          (4500,  9800),
    "sitnik":          (8200,  9000),
    "nadbor":          (7800,  3200),   # sul militar
    "polana":          (3200,  2200),
    "dolnik":          (5000,  1500),   # sul
    "karlin":          (6800,  8800),
    "kolembrody":      (9400,  8500),
    "tarnow":          (3800,  9200),
    # Militar / POI
    "lukow_airfield":  (10800, 7200),
    "dambog_bunker":   ( 1300, 1800),   # underground bunker (pico SW)
    "sowa_hill":       (10400, 2500),   # colina SE
    "rodzanica":       ( 5800, 3000),   # radio base
    "kopa_prison":     ( 7200, 3800),
    "swarog_base":     ( 4600, 2700),
    "krsnik_base":     ( 8700, 3400),
    # Natural
    "bagno_swamp":     ( 1800, 5400),
    "biela_river_mid": ( 6400, 6100),
    "lake_jantar":     ( 7600, 7800),
    "lake_sitnickie":  ( 9000, 8200),
}


@dataclass
class ElevationResult:
    x: float
    z: float
    elevation: float
    zone_name: str
    tier: int
    multiplier: float
    zone_description: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "x": self.x, "z": self.z,
            "elevation_m": round(self.elevation, 2),
            "zone": self.zone_name, "tier": self.tier,
            "price_multiplier": self.multiplier,
            "description": self.zone_description,
        }


class LivoniaHeightmap:
    """
    Heightmap do Livonia (Enoch) com interpolação bilinear e API compatível
    com ChernarusHeightmap para uso em sistema multi-mapa.
    """

    def __init__(self, data: Optional[np.ndarray] = None):
        if data is not None:
            assert data.shape == (GRID_SIZE, GRID_SIZE), \
                f"Esperado ({GRID_SIZE},{GRID_SIZE}), recebido {data.shape}"
            self._data = data.astype(np.float32)
        else:
            self._data = self._generate_fallback()
        print(f"[LivoniaHeightmap] Carregado: min={self._data.min():.1f}m "
              f"max={self._data.max():.1f}m mean={self._data.mean():.1f}m")

    # ── Construtores ──────────────────────────────────────────────────────────

    @classmethod
    def from_asc(cls, filepath: str) -> "LivoniaHeightmap":
        """
        Carrega arquivo ASC exportado pelo Terrain Builder.
        Formato esperado:
            ncols 4096 / nrows 4096 / cellsize 3.125 / xllcorner 200000 / yllcorner 0
        """
        print(f"[LivoniaHeightmap] Carregando ASC: {filepath}")
        header = {}
        with open(filepath, 'r') as f:
            for _ in range(6):
                line = f.readline().strip().split()
                header[line[0].lower()] = float(line[1])
            data = np.loadtxt(f, dtype=np.float32)
        nodata = header.get('nodata_value', -9999)
        data[data == nodata] = 0.0
        data = np.flipud(data)   # ASC norte-primeiro → flip para Z=0 ao sul
        return cls(data)

    @classmethod
    def from_npy(cls, filepath: str) -> "LivoniaHeightmap":
        return cls(np.load(filepath))

    @classmethod
    def from_zone_lookup(cls, filepath: str) -> "LivoniaHeightmap":
        """Reconstrução aproximada a partir do lookup JSON (baixa resolução)."""
        with open(filepath) as f:
            lut = json.load(f)
        chunk = lut["chunk_size_m"]
        cpa = int(MAP_SIZE / chunk)
        low = np.array(lut["elevations"], dtype=np.float32).reshape(cpa, cpa)
        factor = GRID_SIZE // cpa
        data = np.repeat(np.repeat(low, factor, axis=0), factor, axis=1)
        return cls(data)

    # ── Query de elevação ─────────────────────────────────────────────────────

    def get_elevation(self, x: float, z: float) -> float:
        """
        Elevação em metros para coordenadas in-game (X, Z).
        Interpolação bilinear (precisão sub-célula de 3.125m).

        Args:
            x: leste-oeste (0=borda oeste,  12800=borda leste)
            z: sul-norte   (0=borda sul/spawn, 12800=borda norte)
        """
        col_f = max(0.0, min(x / CELL_SIZE, GRID_SIZE - 1.0001))
        row_f = max(0.0, min(z / CELL_SIZE, GRID_SIZE - 1.0001))
        r0, c0 = int(row_f), int(col_f)
        r1, c1 = r0 + 1, c0 + 1
        dr, dc = row_f - r0, col_f - c0
        h = self._data
        return float(
            h[r0, c0] * (1 - dr) * (1 - dc) +
            h[r0, c1] * (1 - dr) * dc +
            h[r1, c0] * dr * (1 - dc) +
            h[r1, c1] * dr * dc
        )

    def get_elevation_batch(self, coords: List[Tuple[float, float]]) -> np.ndarray:
        """Versão vectorizada — ideal para queries em lote no loop do servidor."""
        if not coords:
            return np.array([], dtype=np.float32)
        xs = np.array([c[0] for c in coords], dtype=np.float64)
        zs = np.array([c[1] for c in coords], dtype=np.float64)
        col_f = np.clip(xs / CELL_SIZE, 0.0, GRID_SIZE - 1.0001)
        row_f = np.clip(zs / CELL_SIZE, 0.0, GRID_SIZE - 1.0001)
        r0 = row_f.astype(np.int32)
        c0 = col_f.astype(np.int32)
        r1 = np.minimum(r0 + 1, GRID_SIZE - 1)
        c1 = np.minimum(c0 + 1, GRID_SIZE - 1)
        dr, dc = row_f - r0, col_f - c0
        h = self._data
        return (h[r0, c0].astype(np.float64) * (1 - dr) * (1 - dc) +
                h[r0, c1].astype(np.float64) * (1 - dr) * dc +
                h[r1, c0].astype(np.float64) * dr * (1 - dc) +
                h[r1, c1].astype(np.float64) * dr * dc).astype(np.float32)

    # ── Classificação de zona ─────────────────────────────────────────────────

    def classify_zone(self, x: float, z: float) -> ElevationResult:
        elev = self.get_elevation(x, z)
        for zn, info in ELEVATION_ZONES.items():
            if info["range"][0] <= elev < info["range"][1]:
                return ElevationResult(x, z, elev, zn, info["tier"],
                                       info["multiplier"], info["description"])
        info = ELEVATION_ZONES["pico"]
        return ElevationResult(x, z, elev, "pico", info["tier"],
                               info["multiplier"], info["description"])

    def classify_zone_batch(self, coords: List[Tuple[float, float]]) -> List[ElevationResult]:
        elevs = self.get_elevation_batch(coords)
        results = []
        for (x, z), elev in zip(coords, elevs):
            fe = float(elev)
            for zn, info in ELEVATION_ZONES.items():
                if info["range"][0] <= fe < info["range"][1]:
                    results.append(ElevationResult(x, z, fe, zn, info["tier"],
                                                   info["multiplier"], info["description"]))
                    break
            else:
                info = ELEVATION_ZONES["pico"]
                results.append(ElevationResult(x, z, fe, "pico", info["tier"],
                                               info["multiplier"], info["description"]))
        return results

    # ── Exportação ────────────────────────────────────────────────────────────

    def export_zone_lookup_json(self, chunk_size_m: float = 800.0) -> dict:
        """Gera lookup table compacta (16×16 = 256 chunks de 800m)."""
        cpa = int(MAP_SIZE / chunk_size_m)
        step = GRID_SIZE // cpa
        elevations, zone_names, multipliers, tiers = [], [], [], []
        for row_c in range(cpa):
            for col_c in range(cpa):
                r = min(int((row_c + 0.5) * step), GRID_SIZE - 1)
                c = min(int((col_c + 0.5) * step), GRID_SIZE - 1)
                elev = float(self._data[r, c])
                for zn, info in ELEVATION_ZONES.items():
                    if info["range"][0] <= elev < info["range"][1]:
                        zone_names.append(zn); multipliers.append(info["multiplier"])
                        tiers.append(info["tier"]); break
                else:
                    zone_names.append("pico")
                    multipliers.append(ELEVATION_ZONES["pico"]["multiplier"])
                    tiers.append(ELEVATION_ZONES["pico"]["tier"])
                elevations.append(round(elev, 1))
        return {
            "map": MAP_NAME, "internal_name": INTERNAL, "chunk_size_m": chunk_size_m,
            "chunks_per_axis": cpa, "map_size_m": MAP_SIZE, "cell_size_m": CELL_SIZE,
            "grid_size": GRID_SIZE, "total_chunks": cpa * cpa,
            "usage_formula": f"chunk_index = int(z/{chunk_size_m})*{cpa} + int(x/{chunk_size_m})",
            "usage_python":  f"mult = data['multipliers'][int(z/{chunk_size_m})*{cpa}+int(x/{chunk_size_m})]",
            "usage_js":      f"const m = d.multipliers[Math.floor(z/{chunk_size_m})*{cpa}+Math.floor(x/{chunk_size_m})]",
            "zone_definitions": ELEVATION_ZONES,
            "elevations": elevations, "zone_names": zone_names,
            "multipliers": multipliers, "tiers": tiers,
        }

    def export_known_points_json(self) -> dict:
        pts = {}
        for name, (x, z) in KNOWN_POINTS.items():
            r = self.classify_zone(x, z)
            pts[name] = r.to_dict()
        return {"map": MAP_NAME, "internal_name": INTERNAL, "points": pts}

    def stats(self) -> Dict[str, float]:
        return {
            "map": MAP_NAME, "internal_name": INTERNAL,
            "min_m": float(self._data.min()), "max_m": float(self._data.max()),
            "mean_m": float(self._data.mean()), "std_m": float(self._data.std()),
            "grid_size": GRID_SIZE, "cell_size_m": CELL_SIZE, "map_size_m": MAP_SIZE,
        }

    @staticmethod
    def _generate_fallback() -> np.ndarray:
        print("[LivoniaHeightmap] AVISO: usando heightmap de fallback!")
        rng = np.random.default_rng(0)
        y = np.linspace(0, 250, GRID_SIZE)
        d = np.tile(y.reshape(-1, 1), (1, GRID_SIZE))
        d += rng.uniform(0, 40, d.shape)
        return d.astype(np.float32)

    def __repr__(self):
        return (f"LivoniaHeightmap(Enoch, grid={GRID_SIZE}×{GRID_SIZE}, "
                f"cell={CELL_SIZE}m, map={MAP_SIZE}m, "
                f"elev=[{self._data.min():.0f}-{self._data.max():.0f}]m)")


# ─── Integração com sistema de loja ──────────────────────────────────────────

def compute_shop_price(base_price: float, x: float, z: float,
                       hm: LivoniaHeightmap) -> dict:
    """
    Calcula preço final ajustado por elevação/zona.
    Em Livonia: sul militar = mais perigoso = preços mais altos.
    """
    zone = hm.classify_zone(x, z)
    final = round(base_price * zone.multiplier)
    return {
        "base_price": base_price,
        "final_price": final,
        "multiplier": zone.multiplier,
        "savings": round(final - base_price),
        "location": {
            "x": x, "z": z,
            "elevation_m": round(zone.elevation, 1),
            "zone": zone.zone_name, "tier": zone.tier,
            "region": "norte" if z > 7000 else ("centro" if z > 4500 else "sul"),
        }
    }


def batch_shop_prices(items_coords: List[Tuple[float, float, float]],
                       hm: LivoniaHeightmap) -> List[dict]:
    """
    Calcula preços para múltiplos itens em lote.
    items_coords: lista de (base_price, x, z)
    """
    coords = [(x, z) for _, x, z in items_coords]
    zones = hm.classify_zone_batch(coords)
    return [
        {"base_price": bp, "final_price": round(bp * z.multiplier),
         "zone": z.zone_name, "tier": z.tier, "elevation_m": round(z.elevation, 1)}
        for (bp, _, __), z in zip(items_coords, zones)
    ]


# ─── Demo / Entrypoint ────────────────────────────────────────────────────────

if __name__ == "__main__":
    npy = os.path.join(os.path.dirname(__file__), "livonia_heightmap.npy")
    hm = LivoniaHeightmap.from_npy(npy) if os.path.exists(npy) else LivoniaHeightmap()

    print(f"\n=== {repr(hm)} ===")
    for k, v in hm.stats().items():
        print(f"  {k}: {v}")

    print("\n=== Preços por localização (base R$1000) ===")
    test_pts = [
        ("Topolin (capital norte)",  6200, 11200),
        ("Grabin (cidade)",          4500,  9800),
        ("Lukow Airfield",          10800,  7200),
        ("Nadbor (sul militar)",     7800,  3200),
        ("Kopa Prison",              7200,  3800),
        ("Dambog Bunker (pico)",     1300,  1800),
    ]
    for name, x, z in test_pts:
        r = compute_shop_price(1000, x, z, hm)
        print(f"  {name:30s} → R${r['final_price']:,} "
              f"(x{r['multiplier']} | {r['location']['elevation_m']}m "
              f"| zona: {r['location']['zone']} | {r['location']['region']})")

    print("\n=== Benchmark ===")
    N = 5000
    rng = np.random.default_rng(7)
    xs = rng.uniform(0, MAP_SIZE, N); zs = rng.uniform(0, MAP_SIZE, N)
    t0 = time.perf_counter()
    res = hm.get_elevation_batch(list(zip(xs.tolist(), zs.tolist())))
    t1 = time.perf_counter()
    print(f"  Batch {N} queries: {(t1-t0)*1000:.2f}ms = {N/(t1-t0):.0f} q/s")
