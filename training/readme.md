python -m venv venv
source venv/bin/activate
pip install mlflow torch torch-geometric rdkit pandas scikit-learn fastparquet
mlflow ui

python train.py