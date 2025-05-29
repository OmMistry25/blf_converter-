#!/usr/bin/env python3
"""
BLF to CSV Converter Script

This script converts Binary Logging Format (BLF) files to CSV format.
BLF files are commonly used for CAN bus data logging.

Requirements:
    pip install cantools python-can

Usage:
    python blf_to_csv.py input.blf output.csv
"""

import sys
import csv
import argparse
from pathlib import Path

try:
    import can
    from can import BLFReader
except ImportError:
    print("Error: Required packages not installed.")
    print("Please install with: pip install cantools python-can")
    sys.exit(1)


def convert_blf_to_csv(blf_file_path, csv_file_path, include_error_frames=False):
    """
    Convert BLF file to CSV format.
    
    Args:
        blf_file_path (str): Path to input BLF file
        csv_file_path (str): Path to output CSV file
        include_error_frames (bool): Whether to include error frames in output
    """
    
    try:
        # Open BLF file for reading
        with open(blf_file_path, 'rb') as blf_file:
            reader = BLFReader(blf_file)
            
            # Open CSV file for writing
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
                # Define CSV headers
                fieldnames = [
                    'timestamp',
                    'channel',
                    'arbitration_id',
                    'is_extended_id',
                    'is_remote_frame',
                    'is_error_frame',
                    'dlc',
                    'data'
                ]
                
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                
                message_count = 0
                error_count = 0
                
                # Process each message in the BLF file
                for msg in reader:
                    # Skip error frames if not requested
                    if msg.is_error_frame and not include_error_frames:
                        error_count += 1
                        continue
                    
                    # Convert data bytes to hex string
                    data_hex = ' '.join([f'{byte:02X}' for byte in msg.data]) if msg.data else ''
                    
                    # Write message data to CSV
                    row = {
                        'timestamp': f'{msg.timestamp:.6f}',
                        'channel': msg.channel if hasattr(msg, 'channel') else 'N/A',
                        'arbitration_id': f'0x{msg.arbitration_id:X}',
                        'is_extended_id': str(msg.is_extended_id),
                        'is_remote_frame': str(msg.is_remote_frame),
                        'is_error_frame': str(msg.is_error_frame),
                        'dlc': msg.dlc,
                        'data': data_hex
                    }
                    
                    writer.writerow(row)
                    message_count += 1
                    
                    # Progress indicator for large files
                    if message_count % 10000 == 0:
                        print(f"Processed {message_count} messages...")
                
                print(f"\nConversion completed successfully!")
                print(f"Messages processed: {message_count}")
                if error_count > 0:
                    print(f"Error frames skipped: {error_count}")
                print(f"Output file: {csv_file_path}")
                
    except FileNotFoundError:
        print(f"Error: BLF file '{blf_file_path}' not found.")
        return False
    except PermissionError:
        print(f"Error: Permission denied accessing files.")
        return False
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        return False
    
    return True


def main():
    """Main function to handle command line arguments and execute conversion."""
    
    parser = argparse.ArgumentParser(
        description='Convert BLF (Binary Logging Format) files to CSV format',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        'input_file',
        type=str,
        help='Path to input BLF file'
    )
    
    parser.add_argument(
        'output_file',
        type=str,
        nargs='?',
        help='Path to output CSV file (optional, defaults to input filename with .csv extension)'
    )
    
    parser.add_argument(
        '--include-errors',
        action='store_true',
        help='Include error frames in the output CSV'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='BLF to CSV Converter v1.0'
    )
    
    args = parser.parse_args()
    
    # Validate input file
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file '{args.input_file}' does not exist.")
        return 1
    
    # Determine output file path
    if args.output_file:
        output_path = Path(args.output_file)
    else:
        output_path = input_path.with_suffix('.csv')
    
    print(f"Converting BLF file: {input_path}")
    print(f"Output CSV file: {output_path}")
    print(f"Include error frames: {args.include_errors}")
    print("-" * 50)
    
    # Perform conversion
    success = convert_blf_to_csv(
        str(input_path),
        str(output_path),
        args.include_errors
    )
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())