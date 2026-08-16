import os
import pandas as pd

class FinancialDataExtractionLayer:

    @staticmethod
    def clean_numeric_string(val) -> float:
        """Cleans and converts messy financial text strings into safe float values."""
        if pd.isna(val):
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)

        # Convert string representations like "$1,240,500.00" or " (500) " to raw floats
        val_str = str(val).strip().lower()

        # Remove accounting formatting artifacts
        val_str = (
            val_str.replace("$", "")
            .replace(",", "")
            .replace("%", "")
            .replace(" ", "")
        )

        # Convert accounting parenthesis notation to negative numbers: (500) -> -500
        if val_str.startswith("(") and val_str.endswith(")"):
            val_str = f"-{val_str[1:-1]}"

        try:
            return float(val_str)
        except ValueError:
            # Return 0.0 if the row entry is a text header or completely corrupt string
            return 0.0

    def extract_csv(self, file_path: str) -> dict:
        """Reads a processing CSV file, handles structure noise,
            and returns a clean key-value dictionary of metrics.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Target file not found at: {file_path}")

        # Load CSV with headers
        df = pd.read_csv(file_path, header=0, dtype=str)

        extracted_data = {}

        # Use first column as the key identifier (typically an ID)
        id_column = df.columns[0]

        # Dynamically detect which columns contain numeric data
        # by sampling the first non-empty row
        numeric_columns = []
        sample_row = df.dropna(how='all').iloc[0] if not df.dropna(how='all').empty else None

        if sample_row is not None:
            for col in df.columns:
                if col == id_column:
                    continue
                val = sample_row[col]
                if not pd.isna(val):
                    # Test if the value can be converted to a number
                    test_val = self.clean_numeric_string(val)
                    if test_val != 0.0 or str(val).strip().replace('.', '').replace('-', '').isdigit():
                        numeric_columns.append(col)

        for _, row in df.iterrows():
            # Skip empty rows
            if row.dropna().empty:
                continue

            # Use first column as the key identifier
            row_id = str(row[id_column]).strip()

            # Extract numeric columns for this row
            for col in numeric_columns:
                if col in df.columns and not pd.isna(row[col]):
                    key = f"{row_id}_{col}"
                    cleaned_value = self.clean_numeric_string(row[col])
                    extracted_data[key] = cleaned_value

        return extracted_data


if __name__ == "__main__":
    processing_dir = os.path.join(os.path.dirname(__file__), "data", "processing")
    
    extractor = FinancialDataExtractionLayer()
    
    for filename in os.listdir(processing_dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(processing_dir, filename)
            print(f"Processing {filename}...")
            result = extractor.extract_csv(file_path)
            print(result)
            print("-" * 50)