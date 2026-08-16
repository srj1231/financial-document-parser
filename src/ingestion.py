import datetime
import json
import os
import shutil
import uuid


class DataIngestionLayer:

    def __init__(self, base_dir=None):
        """initializes directory structure and log path"""
        if base_dir is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            base_dir = os.path.join(script_dir, "data")

        self.base_dir = base_dir

        self.landing_dir = os.path.join(self.base_dir, "landing")
        self.processing_dir = os.path.join(self.base_dir, "processing")
        self.archive_dir = os.path.join(self.base_dir, "archive")
        self.failed_dir = os.path.join(self.base_dir, "failed")
        self.log_dir = os.path.join(self.base_dir, "logs")

        # automatically create directories if not created
        for directory in [
            self.landing_dir,
            self.processing_dir,
            self.archive_dir,
            self.failed_dir,
            self.log_dir
        ]:
            os.makedirs(directory, exist_ok=True)   # OS handles exists_ok. It's fast and avoids 2-step check => free from race condition

        # path to a central transaction ledger database
        self.ledger_dir = os.path.join(self.log_dir, "ingestion_ledger.json")


    def _load_ledger(self) -> dict:
        """Helper to read transaction history files."""
        if os.path.exists(self.ledger_dir):
            with open(self.ledger_dir, "r") as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}


    def _write_ledger(self, ledger_data: dict) -> None:
        """Helper to write / update transaction history files."""
        with open(self.ledger_dir, "w") as f:
            json.dump(ledger_data, f, indent=4)


    def scan_and_move_to_processing_dir(self) -> list:
        """Scans the landing directory, register files with UUIDs, and moves to processing directory."""
        files = [
            f
            for f in os.listdir(self.landing_dir)
            if os.path.isfile(os.path.join(self.landing_dir, f))
        ]

        ledger_data = self._load_ledger()
        active_transactions = []

        if not files:
            print("No new files detected in the landing zone.")
            return []

        print(f"Found {len(files)} new files detected in the landing zone.")

        for filename in files:
            # unique immutable tracking tokens
            transaction_id = str(uuid.uuid4())
            file_extension = os.path.splitext(filename)[1]
            source_path = os.path.join(self.landing_dir, filename)
            target_path = os.path.join(self.processing_dir, f"{transaction_id}{file_extension}")

            # build metadata schema block for audit
            metadata = {
                "transaction_id": transaction_id,
                "original_filename": filename,
                "file_extension": file_extension,
                "received_at": datetime.datetime.now().isoformat(),
                "status": "PROCESSING",
                "current_path": target_path,
                "error_message": None
            }

            # move to stage processing atomically
            try:
                shutil.move(source_path, target_path)
                ledger_data[transaction_id] = metadata  # commit metadata to ledger
                active_transactions.append(transaction_id)
                print(f"Added {transaction_id} : {filename} to the processing directory.")

            except Exception as e:
                print(f"Error adding {transaction_id} : {filename} to the processing directory.")

        self._write_ledger(ledger_data)
        return active_transactions


    def update_transaction_status(self, transaction_id: str, status: str, error_msg: str = None):
        """Moves files to the final stage - archive / failed, depending on the downstream results."""
        ledger_data = self._load_ledger()
        if transaction_id not in ledger_data:
            print(f"Transaction id {transaction_id} not in ledger.")
            return

        metadata = ledger_data[transaction_id]
        current_file_path = metadata['current_path']
        file_extension = metadata['file_extension']

        if not os.path.exists(current_file_path):
            print(f"File {current_file_path} missing from the processing directory.")
            return

        # Determine final folder home based on code status outcomes
        if status == "COMPLETED":
            final_destination = os.path.join(
                self.archive_dir, f"{transaction_id}{file_extension}"
            )
            metadata["status"] = "COMPLETED"
        else:
            final_destination = os.path.join(
                self.failed_dir, f"{transaction_id}{file_extension}"
            )
            metadata["status"] = "FAILED"
            metadata["error_message"] = error_msg

        try:
            # move file out of the processing directory
            shutil.move(current_file_path, final_destination)
            metadata["current_path"] = final_destination
            ledger_data[transaction_id] = metadata
            print(f"Added {transaction_id} : {final_destination} to the final directory with status {status}.")
        except Exception as e:
            print(f"Failed executing final step move: {str(e)}")

        self._write_ledger(ledger_data)

if __name__ == "__main__":
    ingestor = DataIngestionLayer()

    # Step 1: ingest data # todo

    # Step 2: scan ingested files and move to processing state
    jobs = ingestor.scan_and_move_to_processing_dir()

    # Step 3: simulate downstream results # todo
    if jobs:
        # Simulate the first file completing successfully
        ingestor.update_transaction_status(jobs[0], "COMPLETED")

        if(len(jobs) > 1):
            ingestor.update_transaction_status(
                jobs[1],
                "FAILED",
                error_msg="ParserException: Row 4 columns mismatched."
            )

