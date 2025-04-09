import os
import json
import pickle
import logging
from datetime import datetime
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CheckpointManager:
    """
    Manages checkpoints for resuming processing from a known good state
    """
    
    def __init__(self, checkpoint_dir="./checkpoints"):
        """
        Initialize the checkpoint manager
        
        Args:
            checkpoint_dir: Directory to store checkpoint files
        """
        self.checkpoint_dir = checkpoint_dir
        
        # Create checkpoint directory if it doesn't exist
        os.makedirs(checkpoint_dir, exist_ok=True)
        
        logger.info(f"Checkpoint manager initialized with directory: {checkpoint_dir}")
    
    def save_checkpoint(self, name, data, format="json"):
        """
        Save a checkpoint
        
        Args:
            name: Checkpoint name
            data: Data to save
            format: Format to save in (json or pickle)
            
        Returns:
            Path to the saved checkpoint file
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        checkpoint_id = f"{name}_{timestamp}"
        
        # Create a subdirectory for this checkpoint type if it doesn't exist
        checkpoint_subdir = os.path.join(self.checkpoint_dir, name)
        os.makedirs(checkpoint_subdir, exist_ok=True)
        
        # Determine file extension and save method based on format
        if format.lower() == "json":
            file_ext = "json"
            checkpoint_path = os.path.join(checkpoint_subdir, f"{checkpoint_id}.{file_ext}")
            
            try:
                with open(checkpoint_path, 'w') as f:
                    json.dump(data, f, indent=2)
            except Exception as e:
                logger.error(f"Failed to save checkpoint {checkpoint_id}: {str(e)}")
                return None
                
        elif format.lower() == "pickle":
            file_ext = "pkl"
            checkpoint_path = os.path.join(checkpoint_subdir, f"{checkpoint_id}.{file_ext}")
            
            try:
                with open(checkpoint_path, 'wb') as f:
                    pickle.dump(data, f)
            except Exception as e:
                logger.error(f"Failed to save checkpoint {checkpoint_id}: {str(e)}")
                return None
                
        else:
            logger.error(f"Unsupported checkpoint format: {format}")
            return None
        
        # Create a metadata file with additional information
        metadata = {
            "checkpoint_id": checkpoint_id,
            "name": name,
            "timestamp": timestamp,
            "format": format,
            "size_bytes": os.path.getsize(checkpoint_path),
            "path": checkpoint_path
        }
        
        metadata_path = os.path.join(checkpoint_subdir, f"{checkpoint_id}.meta")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # Create a 'latest' symlink or reference file
        latest_path = os.path.join(checkpoint_subdir, "latest")
        with open(latest_path, 'w') as f:
            f.write(checkpoint_id)
        
        logger.info(f"Checkpoint {checkpoint_id} saved successfully")
        return checkpoint_path
    
    def load_checkpoint(self, name, checkpoint_id=None, format="json"):
        """
        Load a checkpoint
        
        Args:
            name: Checkpoint name
            checkpoint_id: Specific checkpoint ID to load, or None for latest
            format: Format the checkpoint was saved in
            
        Returns:
            Loaded checkpoint data, or None if not found
        """
        checkpoint_subdir = os.path.join(self.checkpoint_dir, name)
        
        # Determine which checkpoint to load
        if checkpoint_id is None:
            # Load the latest checkpoint
            latest_path = os.path.join(checkpoint_subdir, "latest")
            if not os.path.exists(latest_path):
                logger.warning(f"No latest checkpoint found for {name}")
                return None
                
            with open(latest_path, 'r') as f:
                checkpoint_id = f.read().strip()
        
        # Determine file extension based on format
        if format.lower() == "json":
            file_ext = "json"
        elif format.lower() == "pickle":
            file_ext = "pkl"
        else:
            logger.error(f"Unsupported checkpoint format: {format}")
            return None
        
        # Load the checkpoint file
        checkpoint_path = os.path.join(checkpoint_subdir, f"{checkpoint_id}.{file_ext}")
        if not os.path.exists(checkpoint_path):
            logger.warning(f"Checkpoint file not found: {checkpoint_path}")
            return None
        
        try:
            if format.lower() == "json":
                with open(checkpoint_path, 'r') as f:
                    data = json.load(f)
            else:  # pickle
                with open(checkpoint_path, 'rb') as f:
                    data = pickle.load(f)
                    
            logger.info(f"Checkpoint {checkpoint_id} loaded successfully")
            return data
            
        except Exception as e:
            logger.error(f"Failed to load checkpoint {checkpoint_id}: {str(e)}")
            return None
    
    def list_checkpoints(self, name=None):
        """
        List available checkpoints
        
        Args:
            name: Optional checkpoint name to filter by
            
        Returns:
            List of checkpoint metadata
        """
        checkpoints = []
        
        if name:
            # List checkpoints for a specific name
            checkpoint_subdir = os.path.join(self.checkpoint_dir, name)
            if not os.path.exists(checkpoint_subdir):
                logger.warning(f"No checkpoints found for {name}")
                return []
                
            # Find all metadata files
            for filename in os.listdir(checkpoint_subdir):
                if filename.endswith(".meta"):
                    try:
                        with open(os.path.join(checkpoint_subdir, filename), 'r') as f:
                            metadata = json.load(f)
                            checkpoints.append(metadata)
                    except Exception as e:
                        logger.error(f"Failed to read metadata file {filename}: {str(e)}")
        else:
            # List all checkpoints
            for dirname in os.listdir(self.checkpoint_dir):
                subdir_path = os.path.join(self.checkpoint_dir, dirname)
                if os.path.isdir(subdir_path):
                    for filename in os.listdir(subdir_path):
                        if filename.endswith(".meta"):
                            try:
                                with open(os.path.join(subdir_path, filename), 'r') as f:
                                    metadata = json.load(f)
                                    checkpoints.append(metadata)
                            except Exception as e:
                                logger.error(f"Failed to read metadata file {filename}: {str(e)}")
        
        # Sort by timestamp (newest first)
        checkpoints.sort(key=lambda x: x["timestamp"], reverse=True)
        return checkpoints
    
    def delete_checkpoint(self, name, checkpoint_id):
        """
        Delete a specific checkpoint
        
        Args:
            name: Checkpoint name
            checkpoint_id: Checkpoint ID to delete
            
        Returns:
            True if successful, False otherwise
        """
        checkpoint_subdir = os.path.join(self.checkpoint_dir, name)
        
        # Check if the checkpoint exists
        metadata_path = os.path.join(checkpoint_subdir, f"{checkpoint_id}.meta")
        if not os.path.exists(metadata_path):
            logger.warning(f"Checkpoint {checkpoint_id} not found")
            return False
        
        try:
            # Read metadata to get file format
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            
            # Determine file extension
            file_ext = "json" if metadata["format"].lower() == "json" else "pkl"
            checkpoint_path = os.path.join(checkpoint_subdir, f"{checkpoint_id}.{file_ext}")
            
            # Delete checkpoint and metadata files
            if os.path.exists(checkpoint_path):
                os.remove(checkpoint_path)
            os.remove(metadata_path)
            
            # Update latest reference if needed
            latest_path = os.path.join(checkpoint_subdir, "latest")
            if os.path.exists(latest_path):
                with open(latest_path, 'r') as f:
                    latest_id = f.read().strip()
                
                if latest_id == checkpoint_id:
                    # Find the next latest checkpoint
                    checkpoints = self.list_checkpoints(name)
                    if checkpoints and len(checkpoints) > 0:
                        # The first one is the latest (except the one we just deleted)
                        for cp in checkpoints:
                            if cp["checkpoint_id"] != checkpoint_id:
                                with open(latest_path, 'w') as f:
                                    f.write(cp["checkpoint_id"])
                                break
                    else:
                        # No more checkpoints, remove the latest file
                        os.remove(latest_path)
            
            logger.info(f"Checkpoint {checkpoint_id} deleted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete checkpoint {checkpoint_id}: {str(e)}")
            return False