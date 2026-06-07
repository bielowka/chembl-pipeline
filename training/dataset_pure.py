# dataset_pure.py
import os
import pandas as pd
import torch
import numpy as np
from torch_geometric.data import InMemoryDataset, Data
from rdkit import Chem


class PureDrugDataset(InMemoryDataset):
    def __init__(self, root, parquet_path, transform=None, pre_transform=None):
        self.parquet_path = parquet_path
        super(PureDrugDataset, self).__init__(root, transform, pre_transform)
        # UWAGA: weights_only=False (Zabezpieczenie z PyTorch 2.6+)
        self.data, self.slices = torch.load(self.processed_paths[0], weights_only=False)

    @property
    def raw_file_names(self):
        return [os.path.basename(self.parquet_path)]

    @property
    def processed_file_names(self):
        # INNA NAZWA PLIKU .PT, aby nie nadpisać zbioru z Morganem!
        return ['optimized_dataset_pure.pt']

    def download(self):
        pass

    def process(self):
        print(f"🔥 Rozpoczynam wstępne przetwarzanie pliku (CZYSTA TOPOLOGIA) {self.parquet_path}...")

        df = pd.read_parquet(self.parquet_path)

        ignore_cols = ['canonical_smiles', 'target', 'graph_data', 'chembl_id']
        feature_cols = [c for c in df.columns if c not in ignore_cols]
        for col in feature_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        data_list = []
        total_rows = len(df)

        for idx, row in df.iterrows():
            if idx % 10000 == 0:
                print(f"Postęp: {idx} / {total_rows} cząsteczek...")

            try:
                smiles = row['canonical_smiles']
                target = row['target']
                # Pobieramy TYLKO 21 cech tabelarycznych (bez Morgana)
                tab_x = row[feature_cols].values.astype(float)

                mol = Chem.MolFromSmiles(smiles)
                if not mol: continue

                # --- Węzły ---
                node_features = []
                for atom in mol.GetAtoms():
                    feat = [atom.GetAtomicNum(), atom.GetDegree(), atom.GetFormalCharge(),
                            atom.GetNumRadicalElectrons(), atom.GetHybridization().real,
                            int(atom.GetIsAromatic()), atom.GetMass()]
                    node_features.append(feat)
                x = torch.tensor(node_features, dtype=torch.float32)

                # --- Krawędzie ---
                edge_indices, edge_attrs = [], []
                for bond in mol.GetBonds():
                    i, j = bond.GetBeginAtomIdx(), bond.GetEndAtomIdx()
                    b_type = bond.GetBondType()
                    bond_feat = [
                        float(b_type == Chem.rdchem.BondType.SINGLE), float(b_type == Chem.rdchem.BondType.DOUBLE),
                        float(b_type == Chem.rdchem.BondType.TRIPLE), float(b_type == Chem.rdchem.BondType.AROMATIC),
                        float(bond.GetIsConjugated())
                    ]
                    edge_indices += [[i, j], [j, i]]
                    edge_attrs += [bond_feat, bond_feat]

                if len(edge_indices) > 0:
                    edge_index = torch.tensor(edge_indices, dtype=torch.long).t().contiguous()
                    edge_attr = torch.tensor(edge_attrs, dtype=torch.float32)
                else:
                    edge_index = torch.empty((2, 0), dtype=torch.long)
                    edge_attr = torch.empty((0, 5), dtype=torch.float32)

                y = torch.tensor([[target]], dtype=torch.float32)
                tabular_x = torch.tensor([tab_x], dtype=torch.float32)

                data = Data(x=x, edge_index=edge_index, edge_attr=edge_attr, y=y, tabular_x=tabular_x)
                data_list.append(data)

            except Exception as e:
                continue

        print("✅ Generowanie grafów CZYSTEJ TOPOLOGII zakończone. Optymalizacja i zapis na dysk...")
        data, slices = self.collate(data_list)
        torch.save((data, slices), self.processed_paths[0])
        print("✅ Zapisano pomyślnie. Plik gotowy do nauki!")