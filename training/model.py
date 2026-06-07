# model.py
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GINEConv, global_add_pool


class HybridGINE(nn.Module):
    def __init__(self, num_node_features, num_edge_features, num_tabular_features, hidden_dim=128):
        super(HybridGINE, self).__init__()
        self.node_emb = nn.Linear(num_node_features, hidden_dim)
        self.edge_emb1 = nn.Linear(num_edge_features, hidden_dim)
        self.edge_emb2 = nn.Linear(num_edge_features, hidden_dim)
        self.edge_emb3 = nn.Linear(num_edge_features, hidden_dim)

        nn1 = nn.Sequential(nn.Linear(hidden_dim, hidden_dim), nn.BatchNorm1d(hidden_dim), nn.ReLU(),
                            nn.Linear(hidden_dim, hidden_dim))
        self.conv1 = GINEConv(nn1, train_eps=True)
        self.bn1 = nn.BatchNorm1d(hidden_dim)

        nn2 = nn.Sequential(nn.Linear(hidden_dim, hidden_dim), nn.BatchNorm1d(hidden_dim), nn.ReLU(),
                            nn.Linear(hidden_dim, hidden_dim))
        self.conv2 = GINEConv(nn2, train_eps=True)
        self.bn2 = nn.BatchNorm1d(hidden_dim)

        nn3 = nn.Sequential(nn.Linear(hidden_dim, hidden_dim), nn.BatchNorm1d(hidden_dim), nn.ReLU(),
                            nn.Linear(hidden_dim, hidden_dim))
        self.conv3 = GINEConv(nn3, train_eps=True)
        self.bn3 = nn.BatchNorm1d(hidden_dim)

        combined_dim = hidden_dim + num_tabular_features
        self.fc1 = nn.Linear(combined_dim, hidden_dim)
        self.dropout = nn.Dropout(0.2)
        self.out = nn.Linear(hidden_dim, 1)

    def forward(self, x, edge_index, edge_attr, batch, tabular_x):
        x = self.node_emb(x)

        edge_feat1 = self.edge_emb1(edge_attr)
        x = F.relu(self.bn1(self.conv1(x, edge_index, edge_feat1)))

        edge_feat2 = self.edge_emb2(edge_attr)
        x = F.relu(self.bn2(self.conv2(x, edge_index, edge_feat2)))

        edge_feat3 = self.edge_emb3(edge_attr)
        x = self.conv3(x, edge_index, edge_feat3)
        self.bn3(x)

        x = global_add_pool(x, batch)
        x = torch.cat([x, tabular_x], dim=1)

        x = F.relu(self.fc1(x))
        x = self.dropout(x)
        return self.out(x)