# Route Priority Policy Comparative Study

- Source (beats_zero_folds): `inventory_state_baseline_20260224_113001_684966.json`
- Source (mean_rmse): `inventory_state_baseline_20260224_113024_935032.json`

| Route | Priority (beats_zero_folds) | Priority (mean_rmse) | Changed | SparseB (bzf/mr) | RollViable RMSE (bzf/mr) |
| --- | --- | --- | :---: | --- | --- |
| CXB-DAC | hold | hold | N | False/False | False/False |
| DAC-CXB | watch | high | Y | False/False | False/True |
| DAC-SPD | watch | watch | N | False/False | False/False |
| SPD-DAC | watch | watch | N | False/False | False/False |
