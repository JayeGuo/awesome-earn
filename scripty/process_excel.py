#!/usr/bin/env python3
"""
Process trading data from Excel file and format it according to specifications.
"""

import pandas as pd
import sys
import os


def process_trading_data(input_file, output_file=None):
    """
    Read trading data from Excel and format it.

    Expected output format:
    币种, 开始, 结束, 多/空, 均价, 总额, 平仓均价, 平仓总额
    """
    try:
        # Read the input Excel file
        df = pd.read_excel(input_file)

        print(f"Loaded {len(df)} rows from {input_file}")
        print(f"\nColumns found: {df.columns.tolist()}")

        # Clean the data - remove rows with NaN in key columns
        df_clean = df.dropna(subset=['委托时间(UTC)', '订单号'])

        # Filter out header rows that appear in data
        df_clean = df_clean[df_clean['订单号'] != '成交时间(UTC)']

        print(f"\nCleaned to {len(df_clean)} valid rows")
        print(f"\nSample data:\n{df_clean.head(10)}")

        # Group by trading pair (合约) to aggregate trades
        trades = []

        for symbol in df_clean['合约'].unique():
            if pd.isna(symbol):
                continue

            symbol_df = df_clean[df_clean['合约'] == symbol].sort_values('委托时间(UTC)')

            # Filter out orders with zero or invalid amounts
            symbol_df = symbol_df[pd.to_numeric(symbol_df['成交额'], errors='coerce') > 0]

            if len(symbol_df) == 0:
                continue

            # Determine direction based on first valid order
            first_order = symbol_df.iloc[0]['买卖']
            is_long = '买' in first_order or '开多' in first_order
            direction = "多" if is_long else "空"

            # Separate open and close orders based on direction
            if is_long:
                # For long: buy to open, sell to close
                open_orders = symbol_df[symbol_df['买卖'].str.contains('买|开多', na=False)]
                close_orders = symbol_df[symbol_df['买卖'].str.contains('卖|平', na=False)]
            else:
                # For short: sell to open, buy to close
                open_orders = symbol_df[symbol_df['买卖'].str.contains('卖|开空', na=False)]
                close_orders = symbol_df[symbol_df['买卖'].str.contains('买|平', na=False)]

            # Calculate opening position statistics
            open_avg_price = 0
            open_total_amount = 0
            start_time = None

            if len(open_orders) > 0:
                start_time = open_orders['委托时间(UTC)'].min()
                open_orders_valid = open_orders.dropna(subset=['成交均价 ', '成交量'])
                if len(open_orders_valid) > 0:
                    total_quantity = pd.to_numeric(open_orders_valid['成交量'], errors='coerce').sum()
                    open_avg_price = (pd.to_numeric(open_orders_valid['成交均价 '], errors='coerce') *
                                    pd.to_numeric(open_orders_valid['成交量'], errors='coerce')).sum() / total_quantity if total_quantity > 0 else 0
                    open_total_amount = pd.to_numeric(open_orders_valid['成交额'], errors='coerce').sum()

            # Calculate closing position statistics
            close_avg_price = 0
            close_total_amount = 0
            end_time = start_time

            if len(close_orders) > 0:
                end_time = close_orders['委托时间(UTC)'].max()
                close_orders_valid = close_orders.dropna(subset=['成交均价 ', '成交量'])
                if len(close_orders_valid) > 0:
                    total_close_quantity = pd.to_numeric(close_orders_valid['成交量'], errors='coerce').sum()
                    close_avg_price = (pd.to_numeric(close_orders_valid['成交均价 '], errors='coerce') *
                                     pd.to_numeric(close_orders_valid['成交量'], errors='coerce')).sum() / total_close_quantity if total_close_quantity > 0 else 0
                    close_total_amount = pd.to_numeric(close_orders_valid['成交额'], errors='coerce').sum()

            # Calculate profit (for short positions, profit logic is reversed)
            if is_long:
                profit_amount = close_total_amount - open_total_amount
            else:
                profit_amount = open_total_amount - close_total_amount

            profit_rate = (profit_amount / open_total_amount * 100) if open_total_amount > 0 else 0

            trades.append({
                '币种': symbol,
                '开始': start_time,
                '结束': end_time,
                '多/空': direction,
                '均价': open_avg_price,
                '总额': open_total_amount,
                '平仓均价': close_avg_price,
                '平仓总额': close_total_amount,
                '收益率': f"{profit_rate:.2f}%",
                '收益总额': profit_amount
            })

        # Create output dataframe
        output_df = pd.DataFrame(trades)

        # Determine output filename if not specified
        if output_file is None:
            # Create out directory if it doesn't exist
            os.makedirs('out', exist_ok=True)

            # Get the base filename from input
            input_basename = os.path.basename(input_file)
            base_name = os.path.splitext(input_basename)[0]
            output_file = f"out/{base_name}_processed.xlsx"

        # Save to Excel
        output_df.to_excel(output_file, index=False)
        print(f"\nProcessed {len(output_df)} trading records")
        print(f"\nOutput preview:\n{output_df}")
        print(f"\nProcessed data saved to: {output_file}")

        return output_df

    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    input_file = "in/2025-10-14-hy.xlsx"

    # Allow custom input file from command line
    if len(sys.argv) > 1:
        input_file = sys.argv[1]

    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]

    process_trading_data(input_file, output_file)
