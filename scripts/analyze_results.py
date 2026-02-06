import argparse
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sklearn.metrics import precision_recall_curve, auc


def load_labeled_pairs(pos_path, neg_path):
    dfs = []
    try:
        print(f"Loading positive pairs from: {pos_path}")
        pos_df = pd.read_csv(pos_path, sep='\t', header=None, names=['path_a', 'path_b'])
        pos_df['label'] = 1
        dfs.append(pos_df)
    except Exception as e:
        print(f"Error loading positive file: {e}")
        return None

    try:
        print(f"Loading negative pairs from: {neg_path}")
        neg_df = pd.read_csv(neg_path, sep='\t', header=None, names=['path_a', 'path_b'])
        neg_df['label'] = 0
        dfs.append(neg_df)
    except Exception as e:
        print(f"Error loading negative file: {e}")
        return None

    if not dfs:
        return None
    gold_df = pd.concat(dfs, ignore_index=True)
    return gold_df


def load_system_output(file_path):
    try:
        df = pd.read_csv(file_path, sep='\t', header=None, names=['path_a', 'path_b', 'score'])
        return df
    except Exception as e:
        print(f"Error loading system output: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Analyze DIRT Algorithm Results')
    parser.add_argument('--pos', required=True, help='Path to Positive Pairs file')
    parser.add_argument('--neg', required=True, help='Path to Negative Pairs file')
    parser.add_argument('--system', required=True, help='Path to System Output file')
    parser.add_argument('--plot', default='pr_curve.png', help='Filename for the PR curve plot')
    args = parser.parse_args()

    gold_df = load_labeled_pairs(args.pos, args.neg)
    system_df = load_system_output(args.system)

    if gold_df is None or system_df is None:
        return

    def make_key(row):
        p1 = str(row['path_a']).strip()
        p2 = str(row['path_b']).strip()
        first, second = sorted([p1, p2])
        return f"{first}|{second}"

    gold_df['key'] = gold_df.apply(make_key, axis=1)
    system_df['key'] = system_df.apply(make_key, axis=1)

    merged = pd.merge(gold_df, system_df[['key', 'score']], on='key', how='left')
    merged['score'] = merged['score'].fillna(0.0)

    y_true = merged['label']
    y_scores = merged['score']

    precision, recall, thresholds = precision_recall_curve(y_true, y_scores)
    # precision/recall are length len(thresholds)+1; use [:-1] to align F1 with thresholds
    f1_scores = 2 * (precision[:-1] * recall[:-1]) / (precision[:-1] + recall[:-1] + 1e-10)
    best_idx = f1_scores.argmax()
    best_f1 = f1_scores[best_idx]
    best_thresh = thresholds[best_idx] if best_idx < len(thresholds) else 0.5

    print(f"\n--- Results ---")
    print(f"Best F1 Score:      {best_f1:.4f}")
    print(f"Optimal Threshold:  {best_thresh:.4f}")

    plt.figure(figsize=(8, 6))
    plt.plot(recall, precision, marker='.', label=f'DIRT (AUC={auc(recall, precision):.2f})')
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title(f'Precision-Recall Curve (F1={best_f1:.2f})')
    plt.savefig(args.plot)
    plt.close()
    print(f"\nGraph saved to: {args.plot}")


if __name__ == "__main__":
    main()
