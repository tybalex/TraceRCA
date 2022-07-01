INVO_FILE=order-other+assurance_cpu_1021
HISTORY_FILE=3

# trace data encoding
python run_trace_encoding.py -i A/uninjection/$HISTORY_FILE.pkl -o dataframe/uninjection-trace/$HISTORY_FILE.pkl

# invocation data encoding
python run_invo_encoding.py -i data/test/$INVO_FILE.pkl -o dataframe/$INVO_FILE.pkl
python run_invo_encoding.py -i A/uninjection/$HISTORY_FILE.pkl -o dataframe/uninjection/$HISTORY_FILE.pkl

# summary of input data
python run_dataset_summary.py -i dataframe/$INVO_FILE.pkl

# feature selection
python run_selecting_features.py -i dataframe/$INVO_FILE.pkl -o output/$INVO_FILE.feature -h dataframe/uninjection/$HISTORY_FILE.pkl

# prepare model
python run_anomaly_detection_prepare_model.py -i dataframe/uninjection/$HISTORY_FILE.pkl -t dataframe/uninjection-trace/$HISTORY_FILE.pkl.npz -o model/history/$HISTORY_FILE.pkl

# run anomaly detection on invocation
python run_anomaly_detection_invo.py -i dataframe/$INVO_FILE.pkl -o anomaly_detection_invo_output/$INVO_FILE.pkl -h dataframe/uninjection/$HISTORY_FILE.pkl -u output/$INVO_FILE.feature -c model/history/$HISTORY_FILE.pkl

# run localization
python run_localization_association_rule_mining_20210516.py -i anomaly_detection_invo_output/$INVO_FILE.pkl -o rca/$INVO_FILE.pkl