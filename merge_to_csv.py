import argparse
import boto3
import pandas as pd


def merge_csv_files(bucket_name, folder_path, output_file_path):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects(Bucket=bucket_name, Prefix=folder_path)
    merged_df = pd.DataFrame()
    for obj in response['Contents']:
        file_path = obj['Key']
        if file_path.endswith('/'):
            continue
        if not file_path.endswith('.csv'):
            continue
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(pd.compat.StringIO(csv_content))
        merged_df = pd.concat([merged_df, df])

    merged_df.to_csv(output_file_path, index=False)
    print(f"Merged file saved as {output_file_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Merge CSV files from S3 bucket.')
    parser.add_argument('bucket_name', type=str,
                        help='the name of the S3 bucket')
    parser.add_argument('folder_path', type=str,
                        help='the folder path within the S3 bucket')
    parser.add_argument('output_file_path', type=str,
                        help='the output file path', default="merged_output.csv")

    args = parser.parse_args()
    merge_csv_files(args.bucket_name, args.folder_path, args.output_file_path)
