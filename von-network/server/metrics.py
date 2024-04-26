import logging
import json
from datetime import datetime

LOGGER = logging.getLogger(__name__)

ledger_names = ['ledger', 'pool', 'config', 'audit']
date_fmt = '%Y-%m-%d %H:%M:%S+00:00'

def generate_metrics(validator_info):
    metrics = []

    metrics.append("#TYPE indynode_uptime gauge")
    metrics.append("#TYPE indynode_replicas gauge")
    metrics.append("#TYPE indynode_synced gauge")
    metrics.append("#TYPE indynode_uncommited_txns gauge")
    metrics.append("#TYPE indynode_ic_queue_length gauge")
    metrics.append("#TYPE indynode_pool_total_nodes_count gauge")
    metrics.append("#TYPE indynode_pool_reachable_nodes_count gauge")
    metrics.append("#TYPE indynode_pool_unreachable_nodes_count gauge")
    metrics.append("#TYPE indynode_pool_blacklisted_nodes_count gauge")

    for obj in validator_info:
        if "Node_info" in obj:
            node = obj["Node_info"]["Name"].lower()
            metrics.append("indynode_timestamp{node=\"" + node + "\"} " + str(obj["timestamp"]))
            metrics.append("indynode_delta{node=\"" + node + "\"} " + str(obj["Node_info"]["Metrics"]["Delta"]))
            metrics.append("indynode_lambda{node=\"" + node + "\"} " + str(obj["Node_info"]["Metrics"]["Lambda"]))
            metrics.append("indynode_omega{node=\"" + node + "\"} " + str(obj["Node_info"]["Metrics"]["Omega"]))
            uptime = obj["Node_info"]["Metrics"]["uptime"]
            metrics.append("indynode_uptime{node=\"" + node + "\"} " + str(uptime if uptime is not None else 0))
            replicas = obj["Node_info"]["Count_of_replicas"]
            metrics.append("indynode_replicas{node=\"" + node + "\"} " + str(replicas if replicas is not None else 0))
            metrics.append("indynode_master_instance_started{node=\"" + node + "\"} "+ str(obj["Node_info"]["Metrics"]["instances started"]["0"]))
            #metrics.append("indynode_client_instance_started{node=\"" + node + "\"} "+ str(obj["Node_info"]["Metrics"]["instances started"]["1"]))
            metrics.append("indynode_master_ordered_request_counts{node=\"" + node + "\"} "+ str(obj["Node_info"]["Metrics"]["ordered request counts"]["0"]))
            #metrics.append("indynode_client_ordered_request_counts{node=\"" + node + "\"} "+ str(obj["Node_info"]["Metrics"]["ordered request counts"]["1"]))
            throughput = obj["Node_info"]["Metrics"]["throughput"]["0"]
            metrics.append("indynode_master_throughput{node=\"" + node + "\"} "+ str(throughput if throughput is not None else 0))
            #metrics.append("indynode_client_throughput{node=\"" + node + "\"} "+ str(obj["Node_info"]["Metrics"]["throughput"]["1"]))
            metrics.append("indynode_ic_queue_length{node=\"" + node + "\"} " + str(len(obj["Node_info"]["View_change_status"]["IC_queue"])))
            for k,v in obj["Node_info"]["Metrics"]["transaction-count"].items():
                metrics.append("indynode_transactions{node=\"" + node + "\",ledger=\"" +  k +"\"} "+ str(v))
            metrics.append("indynode_synced{node=\"" + node + "\",ledger=\"ledger\"} " + str(int(obj["Node_info"]["Catchup_status"]["Ledger_statuses"]["0"] == "synced")))
            node_stack_messages_processed = obj["Node_info"]["Metrics"].get("NODE_STACK_MESSAGES_PROCESSED")
            metrics.append(f"indynode_node_stack_messages_processed{{node=\"{node}\"}} {node_stack_messages_processed}")

            client_stack_messages_processed = obj["Node_info"]["Metrics"].get("CLIENT_STACK_MESSAGES_PROCESSED", 0)
            metrics.append(f"indynode_client_stack_messages_processed{{node=\"{node}\"}} {client_stack_messages_processed}")
            metrics.append("indynode_synced{node=\"" + node + "\",ledger=\"pool\"} " + str(int(obj["Node_info"]["Catchup_status"]["Ledger_statuses"]["1"] == "synced")))
           
            metrics.append("indynode_synced{node=\"" + node + "\",ledger=\"config\"} " + str(int(obj["Node_info"]["Catchup_status"]["Ledger_statuses"]["2"] == "synced")))
            metrics.append("indynode_synced{node=\"" + node + "\",ledger=\"audit\"} " + str(int(obj["Node_info"]["Catchup_status"]["Ledger_statuses"]["3"] == "synced")))

            for ledger in ledger_names:
                uncommitted_txns = obj["Node_info"]["Uncommitted_ledger_txns"].get(ledger, {}).get("Count", 0)
                metrics.append(f"indynode_uncommited_txns{{node=\"{node}\",ledger=\"{ledger}\"}} {uncommitted_txns}")

                txn_in_catchup = obj["Node_info"]["Catchup_status"]["Number_txns_in_catchup"].get(ledger, 0)
                metrics.append(f"indynode_txn_in_catchup{{node=\"{node}\",ledger=\"{ledger}\"}} {txn_in_catchup}")

                last_updated_time = obj["Node_info"]["Freshness_status"].get(ledger, {}).get("Last_updated_time")
                if last_updated_time:
                    last_updated_timestamp = datetime.strptime(last_updated_time, date_fmt).timestamp()
                    metrics.append(f"indynode_last_updated{{node=\"{node}\",ledger=\"{ledger}\"}} {last_updated_timestamp}")

                has_write_consensus = obj["Node_info"]["Freshness_status"].get(ledger, {}).get("Has_write_consensus")
                metrics.append(f"indynode_has_write_consensus{{node=\"{node}\",ledger=\"{ledger}\"}} {int(has_write_consensus) if has_write_consensus is not None else 0}")

        if "Pool_info" in obj:
            metrics.append("indynode_pool_total_nodes_count{node=\"" + node + "\"} " + str(int(obj["Pool_info"]["Total_nodes_count"])))
            metrics.append("indynode_pool_reachable_nodes_count{node=\"" + node + "\"} " + str(int(obj["Pool_info"]["Reachable_nodes_count"])))
            metrics.append("indynode_pool_unreachable_nodes_count{node=\"" + node + "\"} " + str(int(obj["Pool_info"]["Unreachable_nodes_count"])))
            metrics.append("indynode_pool_blacklisted_nodes_count{node=\"" + node + "\"} " + str(len(obj["Pool_info"]["Blacklisted_nodes"])))

    return "\n".join(metrics)+"\n"
