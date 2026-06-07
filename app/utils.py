"""Pomocnicze funkcje featurization SMILES -> Data + 33-wymiarowy wektor tabelaryczny.

Odtwarza dokładnie schemat z preprocessing/data_platform/data_share/pipeline.py
oraz training/dataset_pure.py, na ktorych wytrenowano model.pth (HybridGINE pure topology).
"""

from __future__ import annotations

import numpy as np
import torch
from rdkit import Chem
from rdkit.Chem import Crippen, Descriptors, Draw, Lipinski
from torch_geometric.data import Data

# ---------------------------------------------------------------------------
# 1. Stale opisujace przestrzen cech (kolejnosc musi byc identyczna jak w treningu)
# ---------------------------------------------------------------------------

# 33 cechy tabelaryczne w tej samej kolejnosci co w all_meta.parquet
TABULAR_FEATURE_COLS: list[str] = [
    "mw_freebase", "alogp", "hba", "hbd", "psa", "rtb", "num_ro5_violations",
    # one-hot standard_type
    "type_AC50", "type_EC50", "type_GI50", "type_IC50", "type_Kd", "type_Ki",
    # one-hot bao_format
    "bao_BAO_0000019", "bao_BAO_0000217", "bao_BAO_0000218", "bao_BAO_0000219",
    "bao_BAO_0000220", "bao_BAO_0000221", "bao_BAO_0000223", "bao_BAO_0000224",
    "bao_BAO_0000225", "bao_BAO_0000249", "bao_BAO_0000251", "bao_BAO_0000252",
    "bao_BAO_0000357", "bao_BAO_0000366",
    # one-hot organism
    "org_Homo sapiens", "org_Human immunodeficiency virus 1", "org_Mus musculus",
    "org_Other", "org_Rattus norvegicus", "org_Unknown",
]
NUM_TABULAR_FEATURES = len(TABULAR_FEATURE_COLS)  # = 33
NUM_NODE_FEATURES = 7
NUM_EDGE_FEATURES = 5

# Statystyki standaryzacji policzone na probie ~3k unikalnych SMILES z all_meta.parquet
# (pipeline standaryzuje mw_freebase, alogp, hbd, rtb przed zapisem do parquet).
SCALER_STATS: dict[str, tuple[float, float]] = {
    "mw_freebase": (445.0266, 122.3477),
    "alogp": (3.8186, 1.8297),
    "hbd": (1.7567, 1.5107),
    "rtb": (6.0070, 3.4881),
}

# Najczestsze kategorie w treningu
DEFAULT_STANDARD_TYPE = "IC50"
DEFAULT_BAO_FORMAT = "BAO_0000219"
DEFAULT_ORGANISM = "Homo sapiens"

ALLOWED_STANDARD_TYPES = ["AC50", "EC50", "GI50", "IC50", "Kd", "Ki"]
ALLOWED_BAO_FORMATS = [
    "BAO_0000019", "BAO_0000217", "BAO_0000218", "BAO_0000219", "BAO_0000220",
    "BAO_0000221", "BAO_0000223", "BAO_0000224", "BAO_0000225", "BAO_0000249",
    "BAO_0000251", "BAO_0000252", "BAO_0000357", "BAO_0000366",
]
ALLOWED_ORGANISMS = [
    "Homo sapiens", "Human immunodeficiency virus 1", "Mus musculus",
    "Rattus norvegicus", "Other", "Unknown",
]


# ---------------------------------------------------------------------------
# 2. Surowe descriptors z RDKit
# ---------------------------------------------------------------------------

def compute_rdkit_descriptors(mol: Chem.Mol) -> dict[str, float]:
    """Zwraca slownik z 7 numerycznymi cechami w skali surowej (przed standaryzacja)."""
    mw = Descriptors.MolWt(mol)
    logp = Crippen.MolLogP(mol)
    hba = Lipinski.NumHAcceptors(mol)
    hbd = Lipinski.NumHDonors(mol)
    psa = Descriptors.TPSA(mol)
    rtb = Descriptors.NumRotatableBonds(mol)
    ro5 = int(mw > 500) + int(logp > 5) + int(hba > 10) + int(hbd > 5)
    return {
        "mw_freebase": float(mw),
        "alogp": float(logp),
        "hba": float(hba),
        "hbd": float(hbd),
        "psa": float(psa),
        "rtb": float(rtb),
        "num_ro5_violations": float(ro5),
    }


def _standardize(value: float, col: str) -> float:
    mean, std = SCALER_STATS[col]
    if std == 0:
        return 0.0
    return (value - mean) / std


# ---------------------------------------------------------------------------
# 3. Budowa wektora tabelarycznego (33 wymiary)
# ---------------------------------------------------------------------------

def build_tabular_vector(
    mol: Chem.Mol,
    standard_type: str = DEFAULT_STANDARD_TYPE,
    bao_format: str = DEFAULT_BAO_FORMAT,
    organism: str = DEFAULT_ORGANISM,
) -> np.ndarray:
    """Tworzy 33-wymiarowy wektor cech tabelarycznych w kolejnosci TABULAR_FEATURE_COLS."""
    desc = compute_rdkit_descriptors(mol)

    feats: dict[str, float] = {c: 0.0 for c in TABULAR_FEATURE_COLS}

    feats["mw_freebase"] = _standardize(desc["mw_freebase"], "mw_freebase")
    feats["alogp"] = _standardize(desc["alogp"], "alogp")
    feats["hbd"] = _standardize(desc["hbd"], "hbd")
    feats["rtb"] = _standardize(desc["rtb"], "rtb")
    feats["hba"] = desc["hba"]
    feats["psa"] = desc["psa"]
    feats["num_ro5_violations"] = desc["num_ro5_violations"]

    st_key = f"type_{standard_type}"
    if st_key in feats:
        feats[st_key] = 1.0
    else:
        feats[f"type_{DEFAULT_STANDARD_TYPE}"] = 1.0

    bao_key = f"bao_{bao_format}"
    if bao_key in feats:
        feats[bao_key] = 1.0
    else:
        feats[f"bao_{DEFAULT_BAO_FORMAT}"] = 1.0

    org_key = f"org_{organism}"
    if org_key in feats:
        feats[org_key] = 1.0
    else:
        feats["org_Other"] = 1.0

    vec = np.array([feats[c] for c in TABULAR_FEATURE_COLS], dtype=np.float32)
    assert vec.shape[0] == NUM_TABULAR_FEATURES
    return vec


# ---------------------------------------------------------------------------
# 4. Budowa grafu PyG (atomy + wiazania), 1:1 jak w dataset_pure.py
# ---------------------------------------------------------------------------

def smiles_to_data(
    smiles: str,
    standard_type: str = DEFAULT_STANDARD_TYPE,
    bao_format: str = DEFAULT_BAO_FORMAT,
    organism: str = DEFAULT_ORGANISM,
) -> Data | None:
    """Zwraca obiekt torch_geometric Data gotowy do `model(...)`. None gdy SMILES jest zly."""
    if not smiles:
        return None
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None

    # Wezly
    node_features = []
    for atom in mol.GetAtoms():
        feat = [
            atom.GetAtomicNum(),
            atom.GetDegree(),
            atom.GetFormalCharge(),
            atom.GetNumRadicalElectrons(),
            atom.GetHybridization().real,
            int(atom.GetIsAromatic()),
            atom.GetMass(),
        ]
        node_features.append(feat)

    if not node_features:
        return None
    x = torch.tensor(node_features, dtype=torch.float32)

    # Krawedzie
    edge_indices, edge_attrs = [], []
    for bond in mol.GetBonds():
        i, j = bond.GetBeginAtomIdx(), bond.GetEndAtomIdx()
        b_type = bond.GetBondType()
        bond_feat = [
            float(b_type == Chem.rdchem.BondType.SINGLE),
            float(b_type == Chem.rdchem.BondType.DOUBLE),
            float(b_type == Chem.rdchem.BondType.TRIPLE),
            float(b_type == Chem.rdchem.BondType.AROMATIC),
            float(bond.GetIsConjugated()),
        ]
        edge_indices += [[i, j], [j, i]]
        edge_attrs += [bond_feat, bond_feat]

    if edge_indices:
        edge_index = torch.tensor(edge_indices, dtype=torch.long).t().contiguous()
        edge_attr = torch.tensor(edge_attrs, dtype=torch.float32)
    else:
        edge_index = torch.empty((2, 0), dtype=torch.long)
        edge_attr = torch.empty((0, NUM_EDGE_FEATURES), dtype=torch.float32)

    tabular_vec = build_tabular_vector(mol, standard_type, bao_format, organism)
    tabular_x = torch.from_numpy(tabular_vec).to(torch.float32).unsqueeze(0)

    return Data(x=x, edge_index=edge_index, edge_attr=edge_attr, tabular_x=tabular_x)


# ---------------------------------------------------------------------------
# 5. Predykcja pIC50 modelem HybridGINE
# ---------------------------------------------------------------------------

@torch.no_grad()
def predict_pic50(
    model,
    smiles: str,
    device,
    standard_type: str = DEFAULT_STANDARD_TYPE,
    bao_format: str = DEFAULT_BAO_FORMAT,
    organism: str = DEFAULT_ORGANISM,
) -> float | None:
    data = smiles_to_data(smiles, standard_type, bao_format, organism)
    if data is None:
        return None

    # Inferencja na pojedynczym grafie -> batch z samych zer
    batch = torch.zeros(data.x.size(0), dtype=torch.long, device=device)
    x = data.x.to(device)
    edge_index = data.edge_index.to(device)
    edge_attr = data.edge_attr.to(device)
    tabular_x = data.tabular_x.to(device)

    # Uwaga: model ma BatchNorm wewnatrz GINEConv. Aby uniknac bledu dla 1 wezla
    # wymuszamy eval() (BN uzyje runing_mean/var).
    print(f"DEBUG: Przekazuję graf do modelu HybridGINE. Liczba węzłów: {data.x.size(0)}", flush=True)
    model.eval()
    pred = model(x, edge_index, edge_attr, batch, tabular_x)
    print(f"DEBUG: Model zwrócił tensor predykcji: {pred.item()}", flush=True)
    return float(pred.view(-1).item())


# ---------------------------------------------------------------------------
# 6. Czysty raport wlasciwosci chemicznych (do drugiego narzedzia agenta)
# ---------------------------------------------------------------------------

def smiles_to_image(smiles: str, size: tuple[int, int] = (400, 400)):
    """Rysuje czasteczke za pomoca RDKit i zwraca obiekt PIL.Image gotowy do st.image()."""
    if not smiles:
        return None
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    return Draw.MolToImage(mol, size=size)


def chem_properties_report(smiles: str) -> dict | None:
    mol = Chem.MolFromSmiles(smiles) if smiles else None
    if mol is None:
        return None
    d = compute_rdkit_descriptors(mol)
    d["smiles"] = Chem.MolToSmiles(mol)
    d["num_atoms"] = mol.GetNumAtoms()
    d["num_bonds"] = mol.GetNumBonds()
    d["num_rings"] = mol.GetRingInfo().NumRings()
    return d
