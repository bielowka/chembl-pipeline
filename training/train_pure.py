# train_pure.py
import torch
import torch.optim as optim
import torch.nn as nn
from torch_geometric.loader import DataLoader
from sklearn.metrics import r2_score
import numpy as np
import mlflow
import mlflow.pytorch
import os

from model import HybridGINE
# UWAGA: Ładujemy nasz nowy Czysty Dataset!
from dataset_pure import PureDrugDataset


def main():
    mlflow.set_tracking_uri("http://127.0.0.1:5000")

    # Trzymamy ten sam eksperyment, żeby wykresy były w jednym miejscu
    mlflow.set_experiment("MPO_Pretraining_Full")

    EPOCHS = 100
    BATCH_SIZE = 256
    LR = 0.001

    print("⏳ Wczytywanie Czystego Datasetu...")
    root_dir = os.path.join(os.getcwd(), 'data')
    parquet_path = os.path.join(root_dir, 'raw', 'all_meta.parquet')

    # Używamy PureDrugDataset (przy 1 odpaleniu znowu go sparsuje, to normalne)
    dataset = PureDrugDataset(root=root_dir, parquet_path=parquet_path)
    dataset = dataset.shuffle()

    val_size = int(len(dataset) * 0.1)
    train_dataset = dataset[val_size:]
    val_dataset = dataset[:val_size]

    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE, shuffle=False)

    def get_device():
        if torch.backends.mps.is_available():
            print("🚀 Używam GPU (Metal Performance Shaders - MPS)")
            return torch.device("mps")
        elif torch.cuda.is_available():
            print("🚀 Używam NVIDIA CUDA")
            return torch.device("cuda")
        else:
            print("⚠️ GPU niedostępne, używam CPU")
            return torch.device("cpu")
    device = get_device()
    print(f"🔥 Sprzęt gotowy: {device}")

    # Wymiar będzie mniejszy (tylko Twoje cechy z dataframe, bez 1024)
    tab_dim = train_dataset[0].tabular_x.shape[1]
    print(f"Wymiar wejścia tabelarycznego: {tab_dim}")

    model = HybridGINE(num_node_features=7, num_edge_features=5, num_tabular_features=tab_dim).to(device)

    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=LR)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=5)

    best_r2 = -float('inf')

    # INNA NAZWA RUNA, by łatwo odróżnić w UI MLflow!
    print("🚀 Rozpoczynam trening CZYSTEJ TOPOLOGII. Śledź wyniki na http://127.0.0.1:5000")
    with mlflow.start_run(run_name="GINE_Pure_Full_600k"):

        mlflow.log_params({
            "epochs": EPOCHS,
            "batch_size": BATCH_SIZE,
            "learning_rate": LR,
            "morgan_fingerprints": False  # Explicitly log that Morgan is OFF
        })

        for epoch in range(EPOCHS):
            model.train()
            train_loss = 0.0
            for data in train_loader:
                data = data.to(device)
                optimizer.zero_grad()
                predictions = model(data.x, data.edge_index, data.edge_attr, data.batch, data.tabular_x)
                loss = criterion(predictions, data.y.view(-1, 1))
                loss.backward()
                optimizer.step()
                train_loss += loss.item() * data.num_graphs
            train_loss /= len(train_loader.dataset)

            model.eval()
            val_loss = 0.0
            all_preds, all_targets = [], []
            with torch.no_grad():
                for data in val_loader:
                    data = data.to(device)
                    predictions = model(data.x, data.edge_index, data.edge_attr, data.batch, data.tabular_x)
                    loss = criterion(predictions, data.y.view(-1, 1))
                    val_loss += loss.item() * data.num_graphs
                    all_preds.extend(predictions.cpu().numpy())
                    all_targets.extend(data.y.view(-1, 1).cpu().numpy())

            val_loss /= len(val_loader.dataset)
            current_r2 = r2_score(np.array(all_targets), np.array(all_preds))
            scheduler.step(val_loss)

            mlflow.log_metrics({
                "train_loss": train_loss,
                "val_loss": val_loss,
                "val_r2": current_r2,
                "lr": optimizer.param_groups[0]['lr']
            }, step=epoch)

            mark = ""
            if current_r2 > best_r2:
                best_r2 = current_r2
                os.makedirs("/Users/szymonbielowka/PycharmProjects/chemblEDA/training/savedmodels_pure", exist_ok=True)
                torch.save(model.state_dict(), f"/Users/szymonbielowka/PycharmProjects/chemblEDA/training/savedmodels_pure/model_epoch_{epoch}.pth")
                mark = " 🌟 Zapisano artefakt!"
                # MLflow automatycznie zapisze nową, lekką wersję wag
                mlflow.pytorch.log_model(model, "best_model_run")

            print(f"Epoka {epoch + 1:03d}/{EPOCHS} | Train Loss: {train_loss:.4f} | Val R2: {current_r2:.4f}{mark}")


if __name__ == "__main__":
    main()