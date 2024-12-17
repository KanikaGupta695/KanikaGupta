import redis
from tqdm import tqdm
import threading
import time

# Function to migrate large keys in a separate thread
def process_large_keys(large_keys, source_redis, target_db_store):
    """
    Handles large keys skipped in the main migration process.

    Args:
        large_keys (list): List of large key names and details.
        source_redis (redis.Redis): Source Redis connection.
        target_db_store (str): Path to a file/database to store large key data.
    """
    print("Processing large keys in a separate thread...")
    with open(target_db_store, "w") as f:
        for key_info in large_keys:
            key = key_info["name"]
            try:
                data_type = source_redis.type(key).decode()
                dumped_value = source_redis.dump(key)
                ttl = source_redis.ttl(key)
                ttl = 0 if ttl == -1 else ttl * 1000  # Adjust TTL to milliseconds
                
                # Store details in a file for manual processing
                f.write(f"Key: {key}, Type: {data_type}, TTL: {ttl}, Size: {key_info['size']} bytes\n")
                print(f"Captured large key: {key} ({key_info['size']} bytes, type: {data_type})")
                
            except Exception as e:
                print(f"Error processing large key '{key}': {e}")
    print("Large key processing complete. Details stored in:", target_db_store)

# Function to migrate Redis keys with multi-threading
def migrate_redis_keys(source_host, source_port, source_password, source_db,
                       target_host, target_port, target_password, target_db,
                       batch_size=1000, size_limit=10 * 1024 * 1024, large_key_file="large_keys.txt"):
    """
    Migrates all keys from one Redis instance to another, skipping large keys
    and processing them in a separate thread.

    Args:
        source_host (str): Source Redis host.
        source_port (int): Source Redis port.
        source_password (str): Source Redis password.
        source_db (int): Source Redis database number.
        target_host (str): Target Redis host.
        target_port (int): Target Redis port.
        target_password (str): Target Redis password.
        target_db (int): Target Redis database number.
        batch_size (int): Batch size for SCAN operation.
        size_limit (int): Maximum size (in bytes) for small keys.
        large_key_file (str): File to store large key details.
    """
    try:
        # Connect to source and target Redis instances
        source_redis = redis.StrictRedis(host=source_host, port=source_port, password=source_password, db=source_db, socket_timeout=60)
        target_redis = redis.StrictRedis(host=target_host, port=target_port, password=target_password, db=target_db, socket_timeout=60)

        print("Connected to source Redis at {}:{}".format(source_host, source_port))
        print("Connected to target Redis at {}:{}".format(target_host, target_port))

        # Initialize SCAN and track large keys
        cursor = 0
        large_keys = []  # To hold large key details
        total_keys = source_redis.dbsize()
        print(f"Found approximately {total_keys} keys to migrate.")

        with tqdm(total=total_keys, desc="Migrating Keys", unit="key") as pbar:
            while True:
                cursor, keys = source_redis.scan(cursor=cursor, count=batch_size)
                if not keys:
                    break

                with target_redis.pipeline() as pipe:
                    for key in keys:
                        try:
                            key_size = source_redis.memory_usage(key) or 0  # Get key size
                            if key_size > size_limit:  # Skip and record large keys
                                large_keys.append({"name": key, "size": key_size})
                                continue
                            
                            # Migrate key
                            dumped_value = source_redis.dump(key)
                            ttl = source_redis.ttl(key)
                            ttl = 0 if ttl == -1 else ttl * 1000
                            pipe.restore(name=key, ttl=ttl, value=dumped_value, replace=True)
                        except Exception as e:
                            print(f"Error migrating key '{key.decode()}': {e}")
                    pipe.execute()

                pbar.update(len(keys))

                if cursor == 0:
                    break

        # Start a separate thread to process large keys
        large_key_thread = threading.Thread(target=process_large_keys, args=(large_keys, source_redis, large_key_file))
        large_key_thread.start()

        print("Key migration completed successfully!")
        large_key_thread.join()  # Wait for the large key thread to finish

    except redis.AuthenticationError:
        print("Authentication error: Please check your Redis passwords.")
    except redis.RedisError as e:
        print("Redis error:", e)
    except Exception as e:
        print("Error:", e)

# Run the migration
if __name__ == "__main__":
    # Source Redis details
    source_host = "redis-13080.c24.us-east-mz-1.ec2.redns.redis-cloud.com"
    source_port = 13080
    source_password = "your_source_password"
    source_db = 0

    # Target Redis details
    target_host = "redis-18346.c103.us-east-1-mz.ec2.redns.redis-cloud.com"
    target_port = 18346
    target_password = "your_target_password"
    target_db = 0

    # Start migration
    migrate_redis_keys(source_host, source_port, source_password, source_db,
                       target_host, target_port, target_password, target_db,
                       batch_size=1000, size_limit=5 * 1024 * 1024, large_key_file="large_keys.txt")
