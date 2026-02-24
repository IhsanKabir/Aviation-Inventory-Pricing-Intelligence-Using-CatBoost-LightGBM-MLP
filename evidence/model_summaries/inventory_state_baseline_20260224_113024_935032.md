# Inventory State Baseline Model Summary

- dataset_path: `output\reports\inventory_state_v2_latest.csv`
- rows_loaded: `11535`
- rows_after_filters: `6434`
- feature_count: `115`
- numeric_features: `90`
- categorical_features: `25`
- party_gap_profile_feature_count: `16`

## Classification (y_next_search_price_move_class)

- rows train/test: `4754` / `1188`
- split timestamp (UTC): `2026-02-23 18:52:56.490609`
- accuracy: `0.9798` (baseline `0.9806`)
- balanced_accuracy: `0.3330` (baseline `0.3333`)
- macro_f1: `0.3299` (baseline `0.3301`)

## Regression (y_next_search_lowest_fare_delta)

- rows train/test: `4754` / `1188`
- split timestamp (UTC): `2026-02-23 18:52:56.490609`
- MAE: `88.63` (baseline `14.44`)
- RMSE: `147.48` (baseline `120.94`)
- MedianAE: `55.76` (baseline `0.00`)
- R2: `-0.4974` (baseline `-0.0069`)

## Two-Stage Regression (move/no-move + moved-row delta)

- rows train/test: `4754` / `1188`
- split timestamp (UTC): `2026-02-23 18:52:56.490609`
- Stage A move threshold (|delta|): `200.0`
- Stage A move-rate train/test: `0.0307` / `0.0194`
- Stage A balanced_accuracy: `0.5209` (baseline `0.5000`)
- Stage A F1(move): `0.0769` (baseline `0.0000`)
- Stage B moved rows train/test: `146` / `23`
- Stage B moved-only MAE: `723.78` (baseline `432.61`)
- Stage B moved-only RMSE: `944.22` (baseline `700.11`)
- Combined delta MAE: `17.10` (zero baseline `14.44`; median baseline `14.44`)
- Combined delta RMSE: `145.13` (zero baseline `120.94`; median baseline `120.94`)
- Combined delta R2: `-0.4500` (zero baseline `-0.0069`; median baseline `-0.0069`)
- Oracle Stage-A gated RMSE (upper bound): `131.38`
- Best threshold by combined RMSE: `0.58` (RMSE `120.94`, MAE `14.44`, pred_moves `0`)
- Best threshold by combined MAE: `0.58` (MAE `14.44`, RMSE `120.94`, pred_moves `0`)
- Best threshold beating zero baseline by RMSE: `none`
- Best threshold beating zero baseline by MAE: `none`

## Route-Specific Threshold Summaries

- route_group: `DAC-SPD,SPD-DAC,DAC-CXB,CXB-DAC`
- min_move_delta: `200.0`
- min_test_moves: `1`
- min_stage_b_moves: `5`
- rolling_viability_rule: `mean_rmse`
- rolling_viability_min_beat_folds: `2`
- stage_a_calibration: `none`
- stage_a_calibration_cv: `3`
- stage_b_model: `ridge`
- feature_ablation: `none`

- Route threshold tuning report CSV will be written as an artifact (combined for route batch, plus route-specific latest when one route is requested).

- Route rolling evaluation CSV will be written as an artifact (combined for route batch, plus route-specific latest when one route is requested).

| Route | Rows | Priority | 2-stage | SparseB | Beats0(RMSE) | Beats0(MAE) | BestThr(RMSE) | BestRMSE | BestThr>0(RMSE) | BestThr(MAE) | BestMAE |
| --- | ---: | --- | --- | :---: | :---: | :---: | ---: | ---: | ---: | ---: | ---: |
| DAC-SPD | 835 | watch | ok | N | N | N | 0.80 | 57.43 |  | 0.80 | 4.70 |
| SPD-DAC | 909 | watch | ok | N | N | N | 0.50 | 54.74 |  | 0.50 | 4.27 |
| DAC-CXB | 2377 | high | ok | N | N | N | 0.50 | 156.29 |  | 0.50 | 23.82 |
| CXB-DAC | 2313 | hold | ok | N | N | N | 0.50 | 118.60 |  | 0.50 | 13.70 |

### Route Rolling Time-Fold Evaluation (Two-Stage)

| Route | Folds Eval/Total | SparseTest | SparseB | AnyBeat0(RMSE) | AnyBeat0(MAE) | RollingViable(RMSE) | RollingViable(MAE) | Mean BestRMSE | Mean ZeroRMSE |
| --- | ---: | ---: | ---: | :---: | :---: | :---: | :---: | ---: | ---: |
| DAC-SPD | 3/4 | 1 | 0 | N | N | N | N | 104.99 | 104.99 |
| SPD-DAC | 1/4 | 1 | 1 | N | N | N | N | 71.55 | 71.55 |
| DAC-CXB | 4/4 | 0 | 0 | Y | Y | Y | Y | 186.55 | 211.73 |
| CXB-DAC | 4/4 | 0 | 0 | N | N | N | N | 94.42 | 94.42 |

### Route Feature Impact (Two-Stage Top Features)

Top features are route-specific model features for the two-stage run. Engineered = matches route/time/fare-ladder engineered feature families.

#### DAC-SPD
- Stage A top engineered features: `num__dac_spd_inv_press_x_dtd`, `num__dac_spd_lowest_open_fare`, `cat__dac_spd_dow_dtd_key_dow2|D15_30`, `num__dac_spd_fare_spread_pct`, `num__fare_ladder_high_low_ratio`
- Stage B top engineered features: `num__fare_ladder_spread_per_step`, `num__fare_ladder_spread_per_open_bucket`, `cat__search_tod_bin_dac_spd_morning_peak`, `cat__search_tod_bin_morning_peak`, `cat__dep_tod_bin_dac_spd_evening`
- Stage A top features (all): `num__lowest_bucket_seat_proxy`, `num__dac_spd_inv_press_x_dtd`, `num__hours_to_departure`, `num__dac_spd_lowest_open_fare`, `cat__dac_spd_dow_dtd_key_dow2|D15_30`, `num__dac_spd_fare_spread_pct`, `num__fare_ladder_high_low_ratio`, `num__dac_spd_fare_spread_abs`
- Stage B top features (all): `num__fare_ladder_spread_per_step`, `num__fare_ladder_spread_per_open_bucket`, `cat__lowest_bucket_code_adt1_SV`, `cat__lowest_open_bucket_code_SV`, `num__dep_time_min`, `cat__lowest_bucket_code_adt1_FL`, `cat__lowest_open_bucket_code_FL`, `num__max_bucket_seat_proxy`

#### SPD-DAC
- Stage A top engineered features: `num__dtd_x_dep_weekday`, `num__fare_ladder_spread_per_step`, `num__fare_ladder_spread_to_low_ratio`, `num__fare_ladder_spread_per_open_bucket`
- Stage B top engineered features: `num__fare_ladder_spread_per_open_bucket`, `cat__search_tod_bin_morning_peak`, `num__fare_ladder_spread_per_step`, `cat__search_tod_bin_late_night`, `cat__dtd_bucket_D0`
- Stage A top features (all): `num__has_bucket_seat_info`, `num__hours_to_departure`, `cat__inventory_proxy_quality_mixed`, `num__lowest_bucket_seat_proxy`, `num__search_hour`, `num__days_to_departure`, `cat__inventory_proxy_quality_missing`, `num__dtd_x_dep_weekday`
- Stage B top features (all): `num__fare_ladder_spread_per_open_bucket`, `cat__lowest_bucket_code_adt1_DS`, `cat__lowest_open_bucket_code_DS`, `num__lowest_bucket_seat_proxy`, `cat__search_tod_bin_morning_peak`, `num__fare_ladder_spread_per_step`, `num__search_hour`, `num__dep_weekday`

#### DAC-CXB
- Stage A top engineered features: `num__fare_ladder_high_low_ratio`, `num__fare_ladder_spread_to_low_ratio`, `num__fare_ladder_spread_per_open_bucket`
- Stage B top engineered features: `cat__dtd_bucket_D31p`, `cat__search_tod_bin_morning_peak`, `cat__dtd_bucket_D1`, `num__dtd_x_is_weekend`, `cat__dtd_bucket_D15_30`
- Stage A top features (all): `num__lowest_bucket_seat_proxy`, `num__fare_adt1`, `num__fare_ladder_high_low_ratio`, `num__fare_ladder_spread_to_low_ratio`, `num__fare_spread_abs`, `num__fare_ladder_spread_per_open_bucket`, `num__lowest_open_fare`, `num__hours_to_departure`
- Stage B top features (all): `num__has_bucket_seat_info`, `num__max_bucket_seat_proxy`, `num__hours_to_departure`, `num__days_to_departure`, `cat__dtd_bucket_D31p`, `num__open_seat_sum`, `num__dep_month`, `num__probe_has_adt2`

#### CXB-DAC
- Stage A top engineered features: `num__fare_ladder_spread_per_open_bucket`, `num__fare_ladder_spread_to_low_ratio`, `num__fare_ladder_high_low_ratio`
- Stage B top engineered features: `num__fare_ladder_spread_to_low_ratio`, `num__fare_ladder_high_low_ratio`, `cat__search_tod_bin_late_morning`, `num__fare_ladder_spread_per_open_bucket`
- Stage A top features (all): `num__lowest_bucket_seat_proxy`, `num__fare_spread_abs`, `num__max_bucket_seat_proxy`, `num__fare_ladder_spread_per_open_bucket`, `num__fare_adt1`, `num__fare_ladder_spread_to_low_ratio`, `num__fare_ladder_high_low_ratio`, `num__fare_spread_pct`
- Stage B top features (all): `num__hours_to_departure`, `num__days_to_departure`, `num__max_bucket_seat_proxy`, `num__lowest_bucket_seat_proxy`, `cat__lowest_open_bucket_code_PR`, `cat__lowest_bucket_code_adt1_PR`, `num__fare_spread_pct`, `num__fare_ladder_spread_to_low_ratio`

