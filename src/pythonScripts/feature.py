# ==============================================================================
# Feature Extraction Script for E-Commerce Session Transactions & Cart Items
# ==============================================================================
# Description:
# Extracts transaction event types, customer user ID, session ID, and the list 
# of cart product IDs per session from e-commerce event log CSV data.
# ==============================================================================

import pandas as pd
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def extract_transaction_features(csv_file_path: str = "events.csv", output_csv_path: str = "extracted_features.csv") -> pd.DataFrame:
    """
    Extracts transaction features from an event log CSV file.
    
    Parameters:
    -----------
    csv_file_path : str
        Path to the raw user events CSV log file (default: 'events.csv').
    output_csv_path : str, optional
        Path to save the generated extracted features CSV dataframe.
        
    Returns:
    --------
    pd.DataFrame:
        DataFrame containing user_id, session_id, event_type, 
        has_transaction, and cart_product_id_list per session.
    """
    if not os.path.exists(csv_file_path):
        # Fallback to user_events.csv if events.csv isn't in current directory
        if os.path.exists("user_events.csv"):
            csv_file_path = "user_events.csv"
        else:
            raise FileNotFoundError(f"Input file '{csv_file_path}' not found in local directory.")

    print(f"Reading event logs from: {csv_file_path}")
    df = pd.read_csv(csv_file_path)

    # Derive session_id from user+date when not present (DB-sourced or Flask-emitted events won't have it)
    if 'session_id' not in df.columns:
        df['session_id'] = df['user_id'].astype(str) + '_' + pd.to_datetime(df['timestamp']).dt.date.astype(str)

    # 1. Clean & Preprocess Customer ID
    # Extract user_id from session_id if missing (e.g., 'u1204-2026-07-10-s0' -> '1204')
    if 'user_id' not in df.columns or df['user_id'].isna().any():
        df['user_id'] = df['user_id'].astype(str)
        extracted_ids = df['session_id'].str.extract(r'^u(\d+)-')[0]
        df['user_id'] = df['user_id'].fillna(extracted_ids)
    
    # Ensure product_id is formatted consistently as string
    df['product_id'] = df['product_id'].fillna('').astype(str).str.replace('.0', '', regex=False)
    
    # 2. Group by Session & Extract Features
    session_features = []
    
    for (session_id, user_id), session_df in df.groupby(['session_id', 'user_id']):
        # Sort chronologically by timestamp
        session_df = session_df.sort_values('timestamp')
        
        # Check transaction presence
        has_transaction = (session_df['event_type'] == 'transaction').any()
        
        # Extract list of product IDs added to cart or bought in session
        cart_events = session_df[session_df['event_type'].isin(['add_to_cart', 'transaction'])]
        cart_product_ids = [pid for pid in cart_events['product_id'].unique() if pid and pid != 'nan']
        
        # Primary Event Type for Session
        if has_transaction:
            primary_event_type = 'transaction'
        elif len(cart_product_ids) > 0:
            primary_event_type = 'add_to_cart'
        else:
            primary_event_type = 'view'
            
        session_features.append({
            'user_id': str(user_id),
            'session_id': session_id,
            'event_type': primary_event_type,
            'has_transaction': has_transaction,
            'cart_product_id_list': cart_product_ids,
            'total_cart_items': len(cart_product_ids),
            'total_session_events': len(session_df),
            'first_timestamp': session_df['timestamp'].iloc[0],
            'last_timestamp': session_df['timestamp'].iloc[-1]
        })
        
    features_df = pd.DataFrame(session_features)
    
    # 3. Save Output to CSV
    if output_csv_path:
        export_df = features_df.copy()
        # Convert list column to JSON format string for standard CSV compatibility
        export_df['cart_product_id_list'] = export_df['cart_product_id_list'].apply(lambda x: json.dumps(x))
        export_df.to_csv(output_csv_path, index=False)
        print(f"Successfully saved extracted features to: {output_csv_path}")
        
    return features_df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-path", default=os.path.join(BASE_DIR, "../..", "sample", "events.csv"))
    parser.add_argument("--output-path", default=os.path.join(BASE_DIR, "../..", "DB", "extracted_features.csv"))
    args = parser.parse_args()
    features = extract_transaction_features(args.events_path, args.output_path)
    print("\nExtracted Features Preview (First 5 Rows):")
    print(features[['user_id', 'session_id', 'event_type', 'cart_product_id_list']].head())