
import asyncio
import asyncpg
import logging
import os
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseRecovery:
    def __init__(self, host='localhost', port=5432, database='insighteye_db', user='insighteye_user', password='insighteye*+#@'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn: Optional[asyncpg.Connection] = None
    
    async def connect(self):
        """Establish database connection."""
        try:
            self.conn = await asyncpg.connect(
                host=self.host, port=self.port, database=self.database,
                user=self.user, password=self.password
            )
            logger.info("✅ Database connection established")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            return False
    
    async def disconnect(self):
        """Close database connection."""
        if self.conn and not self.conn.is_closed():
            await self.conn.close()
            logger.info("Database connection closed")
    
    async def scan_for_backups(self) -> Dict[str, List[str]]:
        """Scan for any backup tables that might contain recoverable data."""
        backup_patterns = ['_backup', '_bak', '_old', '_temp', '_safety_backup']
        found_backups = {}
        
        try:
            # Get all tables in the database
            all_tables = await self.conn.fetch(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
            )
            
            for row in all_tables:
                table_name = row['table_name']
                
                # Check if it matches backup patterns
                for pattern in backup_patterns:
                    if pattern in table_name:
                        # Determine original table name
                        original_table = table_name.split(pattern)[0]
                        if original_table not in found_backups:
                            found_backups[original_table] = []
                        
                        # Get record count
                        try:
                            count = await self.conn.fetchval(f'SELECT COUNT(*) FROM "{table_name}"')
                            found_backups[original_table].append({
                                'backup_table': table_name,
                                'record_count': count,
                                'created': 'unknown'  # Could parse from timestamp in name
                            })
                        except:
                            pass
                        break
            
            return found_backups
            
        except Exception as e:
            logger.error(f"Error scanning for backups: {e}")
            return {}
    
    async def get_table_structure(self, table_name: str) -> List[Dict[str, Any]]:
        """Get the structure of a table."""
        try:
            columns = await self.conn.fetch("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, table_name)
            
            return [dict(col) for col in columns]
        except Exception as e:
            logger.error(f"Error getting structure for table {table_name}: {e}")
            return []
    
    async def compare_table_structures(self, table1: str, table2: str) -> Dict[str, Any]:
        """Compare structures of two tables."""
        struct1 = await self.get_table_structure(table1)
        struct2 = await self.get_table_structure(table2)
        
        cols1 = {col['column_name']: col for col in struct1}
        cols2 = {col['column_name']: col for col in struct2}
        
        common_columns = set(cols1.keys()) & set(cols2.keys())
        only_in_table1 = set(cols1.keys()) - set(cols2.keys())
        only_in_table2 = set(cols2.keys()) - set(cols1.keys())
        
        return {
            'common_columns': list(common_columns),
            'only_in_source': list(only_in_table1),
            'only_in_target': list(only_in_table2),
            'compatible': len(only_in_table1) == 0 and len(only_in_table2) == 0
        }
    
    async def recover_data_from_backup(self, backup_table: str, target_table: str, 
                                     dry_run: bool = True) -> Dict[str, Any]:
        """Recover data from a backup table to the target table."""
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Attempting to recover {target_table} from {backup_table}")
        
        try:
            # Check if both tables exist
            backup_exists = await self.conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                backup_table
            )
            target_exists = await self.conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                target_table
            )
            
            if not backup_exists:
                return {'success': False, 'error': f'Backup table {backup_table} does not exist'}
            
            if not target_exists:
                return {'success': False, 'error': f'Target table {target_table} does not exist'}
            
            # Get record counts
            backup_count = await self.conn.fetchval(f'SELECT COUNT(*) FROM "{backup_table}"')
            target_count = await self.conn.fetchval(f'SELECT COUNT(*) FROM "{target_table}"')
            
            # Compare structures
            structure_comparison = await self.compare_table_structures(backup_table, target_table)
            
            if not structure_comparison['compatible']:
                logger.warning(f"Table structures are not fully compatible:")
                logger.warning(f"  Only in backup: {structure_comparison['only_in_source']}")
                logger.warning(f"  Only in target: {structure_comparison['only_in_target']}")
            
            # Prepare recovery operation
            common_columns = structure_comparison['common_columns']
            if not common_columns:
                return {'success': False, 'error': 'No common columns found between tables'}
            
            columns_str = ', '.join(f'"{col}"' for col in common_columns)
            
            if dry_run:
                return {
                    'success': True,
                    'dry_run': True,
                    'backup_records': backup_count,
                    'target_records': target_count,
                    'common_columns': common_columns,
                    'structure_compatible': structure_comparison['compatible'],
                    'recovery_query': f'INSERT INTO "{target_table}" ({columns_str}) SELECT {columns_str} FROM "{backup_table}"'
                }
            
            # Perform actual recovery
            async with self.conn.transaction():
                # Option 1: Clear target and insert all from backup
                await self.conn.execute(f'DELETE FROM "{target_table}"')
                
                insert_query = f'''
                    INSERT INTO "{target_table}" ({columns_str})
                    SELECT {columns_str} FROM "{backup_table}"
                '''
                
                result = await self.conn.execute(insert_query)
                recovered_count = int(result.split()[-1]) if result.split()[-1].isdigit() else 0
                
                return {
                    'success': True,
                    'dry_run': False,
                    'backup_records': backup_count,
                    'recovered_records': recovered_count,
                    'target_records_before': target_count,
                    'common_columns': common_columns
                }
                
        except Exception as e:
            logger.error(f"Error during recovery: {e}")
            return {'success': False, 'error': str(e)}
    
    async def create_emergency_backup(self) -> Dict[str, Any]:
        """Create emergency backup of all existing data."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_results = []
        
        try:
            # Get all tables with data
            all_tables = await self.conn.fetch(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
            )
            
            async with self.conn.transaction():
                for row in all_tables:
                    table_name = row['table_name']
                    
                    # Skip tables that are already backups
                    if any(pattern in table_name for pattern in ['_backup', '_bak', '_temp', '_safety']):
                        continue
                    
                    try:
                        # Check if table has data
                        count = await self.conn.fetchval(f'SELECT COUNT(*) FROM "{table_name}"')
                        
                        if count > 0:
                            backup_table_name = f"{table_name}_emergency_backup_{timestamp}"
                            
                            # Create backup
                            backup_query = f'CREATE TABLE "{backup_table_name}" AS SELECT * FROM "{table_name}"'
                            await self.conn.execute(backup_query)
                            
                            # Verify
                            backup_count = await self.conn.fetchval(f'SELECT COUNT(*) FROM "{backup_table_name}"')
                            
                            backup_results.append({
                                'original_table': table_name,
                                'backup_table': backup_table_name,
                                'record_count': backup_count,
                                'success': backup_count == count
                            })
                            
                            logger.info(f"✅ Backed up {table_name}: {count} records → {backup_table_name}")
                    
                    except Exception as table_error:
                        logger.error(f"❌ Failed to backup {table_name}: {table_error}")
                        backup_results.append({
                            'original_table': table_name,
                            'backup_table': None,
                            'record_count': 0,
                            'success': False,
                            'error': str(table_error)
                        })
            
            successful_backups = [r for r in backup_results if r['success']]
            return {
                'success': True,
                'timestamp': timestamp,
                'total_tables_processed': len(backup_results),
                'successful_backups': len(successful_backups),
                'backup_results': backup_results
            }
            
        except Exception as e:
            logger.error(f"Emergency backup failed: {e}")
            return {'success': False, 'error': str(e)}
    
    async def full_database_diagnosis(self) -> Dict[str, Any]:
        """Complete database diagnosis."""
        try:
            # Get all tables
            all_tables = await self.conn.fetch(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
            )
            
            table_info = {}
            total_records = 0
            
            for row in all_tables:
                table_name = row['table_name']
                
                try:
                    count = await self.conn.fetchval(f'SELECT COUNT(*) FROM "{table_name}"')
                    structure = await self.get_table_structure(table_name)
                    
                    table_info[table_name] = {
                        'record_count': count,
                        'columns': [col['column_name'] for col in structure],
                        'column_details': structure
                    }
                    
                    total_records += count
                    
                except Exception as e:
                    table_info[table_name] = {'error': str(e)}
            
            # Categorize tables
            core_tables = [
                "users", "workspaces", "user_accounts", "workspace_members",
                "param_stream", "video_stream", "user_tokens", "sessions",
                "notifications", "otps", "token_blacklist", "logs", "security_events"
            ]
            
            existing_core = [t for t in core_tables if t in table_info]
            missing_core = [t for t in core_tables if t not in table_info]
            
            backup_tables = []
            regular_tables = []
            
            for table_name in table_info.keys():
                if any(pattern in table_name for pattern in ['_backup', '_bak', '_temp', '_safety']):
                    backup_tables.append(table_name)
                elif table_name not in core_tables:
                    regular_tables.append(table_name)
            
            return {
                'total_tables': len(all_tables),
                'total_records': total_records,
                'core_tables': {
                    'expected': core_tables,
                    'existing': existing_core,
                    'missing': missing_core
                },
                'backup_tables': backup_tables,
                'other_tables': regular_tables,
                'table_details': table_info
            }
            
        except Exception as e:
            logger.error(f"Database diagnosis failed: {e}")
            return {'error': str(e)}

async def main():
    """Main recovery workflow."""
    print("🔧 InsightEye Database Recovery Tool")
    print("=" * 50)
    
    # Initialize recovery tool
    recovery = DatabaseRecovery()
    
    if not await recovery.connect():
        print("❌ Cannot connect to database. Please check your connection settings.")
        return
    
    try:
        # Step 1: Full diagnosis
        print("\n📊 Performing full database diagnosis...")
        diagnosis = await recovery.full_database_diagnosis()
        
        print(f"Database Status:")
        print(f"  Total tables: {diagnosis.get('total_tables', 0)}")
        print(f"  Total records: {diagnosis.get('total_records', 0)}")
        print(f"  Core tables existing: {len(diagnosis.get('core_tables', {}).get('existing', []))}")
        print(f"  Core tables missing: {len(diagnosis.get('core_tables', {}).get('missing', []))}")
        print(f"  Backup tables found: {len(diagnosis.get('backup_tables', []))}")
        
        if diagnosis.get('core_tables', {}).get('missing'):
            print(f"  Missing core tables: {diagnosis['core_tables']['missing']}")
        
        # Step 2: Scan for backups
        print("\n🔍 Scanning for backup tables...")
        backups = await recovery.scan_for_backups()
        
        if backups:
            print("Found potential backup tables:")
            for original, backup_list in backups.items():
                print(f"  {original}:")
                for backup in backup_list:
                    print(f"    - {backup['backup_table']} ({backup['record_count']} records)")
        else:
            print("  No backup tables found")
        
        # Step 3: Create emergency backup if data exists
        if diagnosis.get('total_records', 0) > 0:
            print(f"\n💾 Creating emergency backup of {diagnosis['total_records']} existing records...")
            backup_result = await recovery.create_emergency_backup()
            
            if backup_result.get('success'):
                print(f"✅ Emergency backup completed:")
                print(f"  Backed up {backup_result['successful_backups']} tables")
                print(f"  Backup timestamp: {backup_result['timestamp']}")
            else:
                print(f"❌ Emergency backup failed: {backup_result.get('error')}")
        
        # Step 4: Recovery options
        if backups:
            print("\n🔄 Recovery Options Available:")
            print("The following recovery operations can be performed:")
            
            for original, backup_list in backups.items():
                for backup in backup_list:
                    if backup['record_count'] > 0:
                        print(f"\n  Recover {original} from {backup['backup_table']}:")
                        
                        # Test recovery (dry run)
                        dry_run_result = await recovery.recover_data_from_backup(
                            backup['backup_table'], original, dry_run=True
                        )
                        
                        if dry_run_result['success']:
                            print(f"    ✅ Compatible: {dry_run_result['structure_compatible']}")
                            print(f"    📊 Backup has {dry_run_result['backup_records']} records")
                            print(f"    📊 Target has {dry_run_result['target_records']} records")
                            print(f"    🔧 Common columns: {len(dry_run_result['common_columns'])}")
                        else:
                            print(f"    ❌ Recovery not possible: {dry_run_result.get('error')}")
        
        # Step 5: Recommendations
        print("\n💡 Recommendations:")
        
        if diagnosis.get('total_records', 0) > 0:
            print("  1. ✅ Emergency backup created - your data is now safe")
        
        if backups:
            print("  2. 🔄 Backup tables found - data recovery is possible")
            print("  3. 📝 Use the recovery functions to restore data")
        
        print("  4. 🛡️  Update your application with the safety fixes")
        print("  5. 🔧 Use CREATE TABLE IF NOT EXISTS in schema files")
        print("  6. 📊 Test initialization with dry-run mode first")
        
    finally:
        await recovery.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
