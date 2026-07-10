"""Shared model definition -- imported by both ml/training/train_gnn.py and
ml/inference/inference_service.py so the architecture can never drift
between what was trained and what serves."""
from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv

DROPOUT = 0.2


class InstitutionalFraudSAGE(torch.nn.Module):
    """GraphSAGE fraud classifier -- architecture per
    docs/specs/POC_Blueprint.md section 3: two SAGEConv layers with max
    aggregation (isolates anomalous neighbor signatures instead of diluting
    them across high-volume legitimate accounts), dense classifier head."""

    def __init__(self, in_channels: int, hidden_channels: int, out_channels: int = 2):
        super().__init__()
        self.conv1 = SAGEConv(in_channels, hidden_channels, aggr="max")
        self.conv2 = SAGEConv(hidden_channels, hidden_channels, aggr="max")
        self.classifier = nn.Linear(hidden_channels, out_channels)

    def forward(self, x, edge_index):
        x = F.relu(self.conv1(x, edge_index))
        x = F.dropout(x, p=DROPOUT, training=self.training)
        x = F.relu(self.conv2(x, edge_index))
        return F.log_softmax(self.classifier(x), dim=1)
