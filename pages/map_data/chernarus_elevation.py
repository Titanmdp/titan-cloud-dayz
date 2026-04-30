"""
chernarus_elevation.py
======================
Módulo otimizado para query de elevação no Chernarus+ (DayZ Standalone)
para uso em sistemas de loja / economia de servidor.

Parâmetros reais do terrain:
  - Grid: 2048×2048 pontos
  - Cell size: 7.5 metros
  - Mapa: 15.360×15.360 metros
  - Coordenada X: leste-oeste (0=oeste)
  - Coordenada Z: sul-norte (0=sul)

Uso rápido:
    from chernarus_elevation import ChernarusHeightmap
    hm = ChernarusHeightmap()
    elev = hm.get_elevation(4500.0, 10200.0)   # X, Z em metros
    zone  = hm.classify_zone(4500.0, 10200.0)  # retorna dict com zona e tier

Carregar dados reais (recomendado):
    hm = ChernarusHeightmap.from_asc("terrain_heightmap.asc")
    hm = ChernarusHeightmap.from_npy("chernarus_heightmap.npy")
"""

from __future__ import annotations
import numpy as np
import json
import os
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any
from functools import lru_cache


# ─── Constantes do Terrain ───────────────────────────────────────────────────

GRID_SIZE   = 2048          # pontos em cada eixo
CELL_SIZE   = 7.5           # metros por célula
MAP_SIZE    = GRID_SIZE * CELL_SIZE   # 15360.0 m
XLLCORNER   = 200_000.0     # offset engine (ArmA coordinate space)
YLLCORNER   = 0.0


# ─── Zonas de elevação para o sistema de loja ────────────────────────────────

ELEVATION_ZONES = {
    "costa":     {"range": (  0,  30), "tier": 1, "multiplier": 1.00, "description": "Planície costeira"},
    "planicie":  {"range": ( 30, 100), "tier": 1, "multiplier": 1.05, "description": "Planície e vales"},
    "colinas":   {"range": (100, 250), "tier": 2, "multiplier": 1.15, "description": "Colinas suaves"},
    "montanha":  {"range": (250, 450), "tier": 3, "multiplier": 1.30, "description": "Região montanhosa"},
    "alto":      {"range": (450, 620), "tier": 4, "multiplier": 1.50, "description": "Altitude alta"},
    "pico":      {"range": (620, 999), "tier": 5, "multiplier": 1.75, "description": "Picos e cumes"},
}

# Pontos de interesse com elevação aproximada (X, Z em metros)
KNOWN_POINTS: Dict[str, Tuple[float, float]] = {
    "chernogorsk":      (3400,  1650),
    "elektrozavodsk":   (5100,  1200),
    "berezino":         (11800, 9100),
    "novodmitrovsk":    (10200, 11900),
    "nwaf":             (1300,  11700),
    "severograd":       (4800,  12200),
    "zelenogorsk":      (850,   8700),
    "tisy_military":    (1900,  12800),
    "gorka":            (8800,  8200),
    "stary_sobor":      (6200,  7800),
    "kabanino":         (5400,  8600),
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
            "x": self.x,
            "z": self.z,
            "elevation_m": round(self.elevation, 2),
            "zone": self.zone_name,
            "tier": self.tier,
            "price_multiplier": self.multiplier,
            "description": self.zone_description,
        }


class ChernarusHeightmap:
    """
    Heightmap do Chernarus+ com interpolação bilinear e cache LRU.
    Thread-safe para leitura (sem writes após __init__).
    """

    def __init__(self, data: Optional[np.ndarray] = None):
        if data is not None:
            assert data.shape == (GRID_SIZE, GRID_SIZE), (
                f"Esperado ({GRID_SIZE},{GRID_SIZE}), recebido {data.shape}"
            )
            self._data = data.astype(np.float32)
        else:
            # Fallback: gera terreno sintético de emergência
            self._data = self._generate_fallback()
        
        self._valid = True
        print(f"[ChernarusHeightmap] Carregado: min={self._data.min():.1f}m "
              f"max={self._data.max():.1f}m mean={self._data.mean():.1f}m")

    # ── Construtores alternativos ────────────────────────────────────────────

    @classmethod
    def from_asc(cls, filepath: str) -> "ChernarusHeightmap":
        """
        Carrega arquivo ASC exportado pelo Terrain Builder do DayZ Tools.
        Formato esperado:
            ncols 2048
            nrows 2048
            xllcorner 200000.000000
            yllcorner 0.000000
            cellsize 7.500000
            NODATA_value -9999
            <2048 linhas de 2048 floats>
        """
        print(f"[ChernarusHeightmap] Carregando ASC: {filepath}")
        header = {}
        with open(filepath, 'r') as f:
            # Ler header (6 linhas)
            for _ in range(6):
                line = f.readline().strip().split()
                header[line[0].lower()] = float(line[1])
            # Ler dados
            data = np.loadtxt(f, dtype=np.float32)
        
        nodata = header.get('nodata_value', -9999)
        data[data == nodata] = 0.0
        
        # ASC é row-major de cima para baixo (norte primeiro)
        # DayZ in-game Z=0 é sul, então flip vertical
        data = np.flipud(data)
        return cls(data)

    @classmethod
    def from_npy(cls, filepath: str) -> "ChernarusHeightmap":
        """Carrega arquivo .npy pré-processado (mais rápido)."""
        data = np.load(filepath)
        return cls(data)

    @classmethod
    def from_json_lookup(cls, filepath: str) -> "ChernarusHeightmap":
        """
        Carrega a lookup table JSON compacta gerada por este módulo.
        Reconstrução aproximada de baixa resolução (útil para preview).
        """
        with open(filepath) as f:
            lut = json.load(f)
        chunk = lut["chunk_size_m"]
        chunks_per_axis = int(MAP_SIZE / chunk)
        data_low = np.array(lut["elevations"], dtype=np.float32).reshape(
            chunks_per_axis, chunks_per_axis
        )
        # Upscale por repeat simples (nearest-neighbor)
        factor = GRID_SIZE // chunks_per_axis
        data = np.repeat(np.repeat(data_low, factor, axis=0), factor, axis=1)
        return cls(data)

    # ── Query de elevação ────────────────────────────────────────────────────

    def get_elevation(self, x: float, z: float) -> float:
        """
        Retorna elevação em metros para coordenadas in-game (X, Z).
        Usa interpolação bilinear para precisão sub-célula.
        
        Args:
            x: coordenada leste-oeste in-game (0 = borda oeste, 15360 = borda leste)
            z: coordenada sul-norte in-game  (0 = borda sul,  15360 = borda norte)
        Returns:
            elevação em metros (float)
        """
        # Converter coordenada de mundo → índice de grid
        col_f = x / CELL_SIZE
        row_f = z / CELL_SIZE
        
        # Clamp nas bordas
        col_f = max(0.0, min(col_f, GRID_SIZE - 1.0001))
        row_f = max(0.0, min(row_f, GRID_SIZE - 1.0001))
        
        r0, c0 = int(row_f), int(col_f)
        r1, c1 = r0 + 1, c0 + 1
        dr = row_f - r0
        dc = col_f - c0
        
        # Interpolação bilinear nos 4 vizinhos
        h00 = float(self._data[r0, c0])
        h01 = float(self._data[r0, c1])
        h10 = float(self._data[r1, c0])
        h11 = float(self._data[r1, c1])
        
        return (h00 * (1 - dr) * (1 - dc) +
                h01 * (1 - dr) * dc +
                h10 * dr * (1 - dc) +
                h11 * dr * dc)

    def get_elevation_batch(self, coords: List[Tuple[float, float]]) -> np.ndarray:
        """
        Versão vectorizada para queries em lote.
        
        Args:
            coords: lista de (x, z) tuplas
        Returns:
            np.ndarray de elevações (float32)
        """
        if not coords:
            return np.array([], dtype=np.float32)
        
        xs = np.array([c[0] for c in coords], dtype=np.float64)
        zs = np.array([c[1] for c in coords], dtype=np.float64)
        
        col_f = np.clip(xs / CELL_SIZE, 0.0, GRID_SIZE - 1.0001)
        row_f = np.clip(zs / CELL_SIZE, 0.0, GRID_SIZE - 1.0001)
        
        r0 = col_f.astype(np.int32)  # nota: col/row intencionalmente trocados para x/z
        # Corrigido:
        r0 = row_f.astype(np.int32)
        c0 = col_f.astype(np.int32)
        r1 = np.minimum(r0 + 1, GRID_SIZE - 1)
        c1 = np.minimum(c0 + 1, GRID_SIZE - 1)
        
        dr = row_f - r0
        dc = col_f - c0
        
        h00 = self._data[r0, c0].astype(np.float64)
        h01 = self._data[r0, c1].astype(np.float64)
        h10 = self._data[r1, c0].astype(np.float64)
        h11 = self._data[r1, c1].astype(np.float64)
        
        result = (h00 * (1 - dr) * (1 - dc) +
                  h01 * (1 - dr) * dc +
                  h10 * dr * (1 - dc) +
                  h11 * dr * dc)
        return result.astype(np.float32)

    # ── Classificação por zona ───────────────────────────────────────────────

    def classify_zone(self, x: float, z: float) -> ElevationResult:
        """
        Retorna a zona de elevação e multiplicador de preço para um ponto.
        Usado pelo sistema de loja para ajustar preços por localização.
        """
        elev = self.get_elevation(x, z)
        
        for zone_name, info in ELEVATION_ZONES.items():
            lo, hi = info["range"]
            if lo <= elev < hi:
                return ElevationResult(
                    x=x, z=z,
                    elevation=elev,
                    zone_name=zone_name,
                    tier=info["tier"],
                    multiplier=info["multiplier"],
                    zone_description=info["description"],
                )
        
        # Fallback para pico máximo
        info = ELEVATION_ZONES["pico"]
        return ElevationResult(
            x=x, z=z, elevation=elev,
            zone_name="pico", tier=info["tier"],
            multiplier=info["multiplier"],
            zone_description=info["description"],
        )

    def classify_zone_batch(self, coords: List[Tuple[float, float]]) -> List[ElevationResult]:
        """Versão em lote de classify_zone."""
        elevs = self.get_elevation_batch(coords)
        results = []
        for (x, z), elev in zip(coords, elevs):
            for zone_name, info in ELEVATION_ZONES.items():
                lo, hi = info["range"]
                if lo <= float(elev) < hi:
                    results.append(ElevationResult(
                        x=x, z=z, elevation=float(elev),
                        zone_name=zone_name, tier=info["tier"],
                        multiplier=info["multiplier"],
                        zone_description=info["description"],
                    ))
                    break
            else:
                info = ELEVATION_ZONES["pico"]
                results.append(ElevationResult(
                    x=x, z=z, elevation=float(elev),
                    zone_name="pico", tier=info["tier"],
                    multiplier=info["multiplier"],
                    zone_description=info["description"],
                ))
        return results

    # ── Exportação de lookup table ───────────────────────────────────────────

    def export_zone_lookup_json(self, chunk_size_m: float = 960.0) -> dict:
        """
        Gera lookup table JSON em chunks para uso em sistemas que não
        querem depender do numpy em produção.
        
        chunk_size_m: tamanho do chunk em metros (default 960m = ~16x16 chunks no mapa)
        """
        chunks_per_axis = int(MAP_SIZE / chunk_size_m)
        step = GRID_SIZE // chunks_per_axis
        
        elevations = []
        zone_names = []
        multipliers = []
        
        for row_chunk in range(chunks_per_axis):
            for col_chunk in range(chunks_per_axis):
                # Centro do chunk
                r = int((row_chunk + 0.5) * step)
                c = int((col_chunk + 0.5) * step)
                r = min(r, GRID_SIZE - 1)
                c = min(c, GRID_SIZE - 1)
                elev = float(self._data[r, c])
                
                zone_n = "pico"
                mult = ELEVATION_ZONES["pico"]["multiplier"]
                for zn, info in ELEVATION_ZONES.items():
                    if info["range"][0] <= elev < info["range"][1]:
                        zone_n = zn
                        mult = info["multiplier"]
                        break
                
                elevations.append(round(elev, 1))
                zone_names.append(zone_n)
                multipliers.append(mult)
        
        return {
            "map": "ChernarusPlus",
            "chunk_size_m": chunk_size_m,
            "chunks_per_axis": chunks_per_axis,
            "map_size_m": MAP_SIZE,
            "cell_size_m": CELL_SIZE,
            "grid_size": GRID_SIZE,
            "total_chunks": chunks_per_axis * chunks_per_axis,
            "usage": (
                f"chunk_index = int(z / {chunk_size_m}) * {chunks_per_axis} + int(x / {chunk_size_m}); "
                "mult = multipliers[chunk_index]"
            ),
            "elevations": elevations,
            "zone_names": zone_names,
            "multipliers": multipliers,
            "zone_definitions": {k: v for k, v in ELEVATION_ZONES.items()},
        }

    def export_known_points_json(self) -> dict:
        """Exporta elevações para todos os pontos de interesse conhecidos."""
        points = {}
        for name, (x, z) in KNOWN_POINTS.items():
            result = self.classify_zone(x, z)
            points[name] = result.to_dict()
        return {
            "map": "ChernarusPlus",
            "description": "Elevacoes e zonas para locais conhecidos",
            "points": points
        }

    # ── Utilitários ──────────────────────────────────────────────────────────

    @staticmethod
    def _generate_fallback() -> np.ndarray:
        """Heightmap mínimo de fallback (gradiente simples)."""
        print("[ChernarusHeightmap] AVISO: usando heightmap de fallback!")
        rng = np.random.default_rng(0)
        y = np.linspace(0, 400, GRID_SIZE)
        h = np.tile(y.reshape(-1, 1), (1, GRID_SIZE))
        h += rng.uniform(0, 50, h.shape)
        return h.astype(np.float32)

    def stats(self) -> Dict[str, float]:
        return {
            "min_m": float(self._data.min()),
            "max_m": float(self._data.max()),
            "mean_m": float(self._data.mean()),
            "std_m": float(self._data.std()),
            "grid_size": GRID_SIZE,
            "cell_size_m": CELL_SIZE,
            "map_size_m": MAP_SIZE,
        }

    def __repr__(self):
        return (f"ChernarusHeightmap(grid={GRID_SIZE}x{GRID_SIZE}, "
                f"cell={CELL_SIZE}m, map={MAP_SIZE}m, "
                f"elev=[{self._data.min():.0f}-{self._data.max():.0f}]m)")


# ─── Exemplo de integração com sistema de loja ───────────────────────────────

def compute_shop_price(base_price: float, x: float, z: float,
                        hm: ChernarusHeightmap) -> dict:
    """
    Calcula o preço final de um item na loja com base na elevação do comprador.
    
    Lógica: itens de sobrevivência ficam mais caros em altitude
    (mais difícil de transportar, mais perigoso).
    
    Returns:
        dict com preço final e detalhes da zona
    """
    zone_result = hm.classify_zone(x, z)
    final_price = round(base_price * zone_result.multiplier)
    
    return {
        "base_price": base_price,
        "final_price": final_price,
        "multiplier": zone_result.multiplier,
        "location": {
            "x": x, "z": z,
            "elevation_m": round(zone_result.elevation, 1),
            "zone": zone_result.zone_name,
            "tier": zone_result.tier,
        }
    }


# ─── Entrypoint de demonstração ──────────────────────────────────────────────

if __name__ == "__main__":
    import os
    
    # Tentar carregar arquivo real, senão usa fallback
    npy_path = os.path.join(os.path.dirname(__file__), "chernarus_heightmap.npy")
    if os.path.exists(npy_path):
        hm = ChernarusHeightmap.from_npy(npy_path)
    else:
        hm = ChernarusHeightmap()  # fallback
    
    print("\n=== Stats do Heightmap ===")
    for k, v in hm.stats().items():
        print(f"  {k}: {v}")
    
    print("\n=== Teste de preço por localização ===")
    item_base = 1000
    test_points = [
        ("Chernogorsk (costa)",   3400,  1650),
        ("Berezino (costa leste)", 11800, 9100),
        ("Zelenogorsk (colinas)",  850,   8700),
        ("NWAF (planalto)",        1300, 11700),
        ("Pico Rog",               4400, 10400),
    ]
    for name, x, z in test_points:
        result = compute_shop_price(item_base, x, z, hm)
        print(f"  {name:30s} -> R${result['final_price']:,} "
              f"(x{result['multiplier']} | {result['location']['elevation_m']}m | "
              f"zona: {result['location']['zone']})")
