//! Durable file-level queue and ledger. Folder JSON contains summaries only.
use std::path::Path;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::atomic::{AtomicUsize, Ordering};
use anyhow::{bail, Result};
use crate::downloader::folder::{PendingFile, SkippedFile};

#[derive(Debug)]
pub struct FolderFileStore { db: Mutex<Connection>, pending: AtomicUsize }

impl FolderFileStore {
    pub fn open(wal: &Path, id: &str, required: bool) -> Result<Self> {
        // IDs are generated UUIDs; never interpolate them into SQL.
        if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c=='-' || c=='_') { bail!("invalid folder id"); }
        let dir = wal.join("folder_files");
        let path = dir.join(format!("{id}.sqlite"));
        if required && !path.is_file() { bail!("文件级数据库缺失: {}", path.display()); }
        std::fs::create_dir_all(dir)?;
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        if required {
            let initialized: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM meta WHERE key='migrated')", [], |r|r.get(0))?;
            if !initialized { bail!("文件级数据库未完成迁移"); }
        }
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-2048;
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY, fs_id TEXT NOT NULL, filename TEXT NOT NULL, remote_path TEXT NOT NULL,
                size INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'pending');
            CREATE INDEX IF NOT EXISTS files_state_path ON files(state,path);
            CREATE INDEX IF NOT EXISTS files_fs ON files(fs_id);
            CREATE TABLE IF NOT EXISTS completed (key TEXT PRIMARY KEY, size INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS completed_tasks (task_id TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS failures (key TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS retries (task_id TEXT PRIMARY KEY, attempts INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS task_paths (task_id TEXT PRIMARY KEY, path TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS skipped (path TEXT PRIMARY KEY, size INTEGER NOT NULL, at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);")?;
        let pending = db.query_row("SELECT COUNT(*) FROM files WHERE state='pending'", [], |r| r.get::<_,i64>(0))? as usize;
        Ok(Self { db: Mutex::new(db), pending: AtomicUsize::new(pending) })
    }

    pub fn migrate(&self, pending: &[PendingFile], done: &std::collections::HashSet<u64>, skipped: &[SkippedFile], summary:(u64,u64,u64,u64)) -> Result<()> {
        let mut db = self.db.lock();
        let tx = db.transaction()?;
        let migrated: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM meta WHERE key='migrated')", [], |r| r.get(0))?;
        if !migrated {
            for (key,value) in [("legacy_done",summary.0),("legacy_bytes",summary.1),("legacy_skips",summary.2),("legacy_skip_bytes",summary.3)] {
                tx.execute("INSERT OR REPLACE INTO meta VALUES(?1,?2)",params![key,value])?;
            }
            for id in done { tx.execute("INSERT OR IGNORE INTO completed VALUES(?1,0)", [format!("fs:{id}")])?; }
            for file in skipped { tx.execute("INSERT OR IGNORE INTO skipped VALUES(?1,?2,?3)", params![file.relative_path, file.size, file.skipped_at])?; }
            for file in pending { Self::insert(&tx, file)?; }
            tx.execute("INSERT INTO meta VALUES('migrated',1)", [])?;
        }
        tx.commit()?;
        self.pending.store(db.query_row("SELECT COUNT(*) FROM files WHERE state='pending'", [], |r| r.get::<_,i64>(0))? as usize, Ordering::SeqCst);
        Ok(())
    }

    fn insert(db: &Connection, f: &PendingFile) -> Result<usize> {
        Ok(db.execute("INSERT OR IGNORE INTO files(path,fs_id,filename,remote_path,size)
            SELECT ?1,?2,?3,?4,?5 WHERE NOT EXISTS(SELECT 1 FROM completed WHERE key=?6)
            AND NOT EXISTS(SELECT 1 FROM completed WHERE key='path:' || ?1)
            AND NOT EXISTS(SELECT 1 FROM skipped WHERE path=?1)",
            params![f.relative_path, f.fs_id.to_string(), f.filename, f.remote_path, f.size, format!("fs:{}", f.fs_id)])?)
    }

    pub fn enqueue(&self, files: &[PendingFile]) -> Result<()> {
        let mut db = self.db.lock(); let tx = db.transaction()?;
        let mut added=0;
        for f in files { added += Self::insert(&tx,f)?; }
        tx.commit()?; self.pending.fetch_add(added,Ordering::SeqCst); Ok(())
    }

    pub fn pending_count(&self) -> usize { self.pending.load(Ordering::SeqCst) }

    pub fn page(&self, limit: usize) -> Result<Vec<PendingFile>> {
        let db = self.db.lock();
        let mut stmt = db.prepare("SELECT fs_id,filename,remote_path,path,size FROM files WHERE state='pending' ORDER BY path LIMIT ?1")?;
        let rows = stmt.query_map([limit as i64], |r| Ok(PendingFile {
            fs_id: r.get::<_,String>(0)?.parse().map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?,
            filename:r.get(1)?, remote_path:r.get(2)?, relative_path:r.get(3)?, size:r.get(4)?
        }))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn mark_active(&self, fs_id: u64, task_id:&str, path:Option<&str>) -> Result<()> {
        let mut db=self.db.lock(); let tx=db.transaction()?;
        if let Some(path)=path { tx.execute("INSERT OR REPLACE INTO task_paths VALUES(?1,?2)",params![task_id,path])?; }
        let changed=if let Some(path)=path {
            tx.execute("UPDATE files SET state='active' WHERE path=?1 AND state='pending'",[path])?
        } else if fs_id != 0 {
            tx.execute("UPDATE files SET state='active' WHERE fs_id=?1 AND state='pending'",[fs_id.to_string()])?
        } else {0};
        tx.commit()?; self.pending.fetch_sub(changed,Ordering::SeqCst);
        Ok(())
    }

    pub fn is_completed(&self, fs_id:u64, task_id:&str) -> Result<bool> {
        let db=self.db.lock();
        if fs_id==0 {
            return Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM completed_tasks WHERE task_id=?1)",[task_id],|r|r.get(0))?);
        }
        Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM completed WHERE key=?1)", [format!("fs:{fs_id}")], |r| r.get(0))?)
    }

    pub fn complete(&self, fs_id:u64, task_id:&str, size:u64) -> Result<bool> {
        let mut db=self.db.lock(); let tx=db.transaction()?;
        let path:Option<String>=tx.query_row("SELECT path FROM task_paths WHERE task_id=?1",[task_id],|r|r.get(0)).optional()?;
        let key=if fs_id==0 {path.as_ref().map_or_else(||format!("task:{task_id}"),|p|format!("path:{p}"))} else {format!("fs:{fs_id}")};
        let inserted=tx.execute("INSERT OR IGNORE INTO completed VALUES(?1,?2)", params![key,size])? != 0;
        if inserted {
            tx.execute("INSERT INTO meta VALUES('live_done',1) ON CONFLICT(key) DO UPDATE SET value=value+1",[])?;
            tx.execute("INSERT INTO meta VALUES('live_bytes',?1) ON CONFLICT(key) DO UPDATE SET value=value+excluded.value",[size])?;
        }
        tx.execute("UPDATE completed SET size=MAX(size,?2) WHERE key=?1", params![key,size])?;
        tx.execute("INSERT OR IGNORE INTO completed_tasks VALUES(?1)",[task_id])?;
        tx.execute("DELETE FROM retries WHERE task_id=?1",[task_id])?;
        tx.execute("DELETE FROM failures WHERE key=?1 OR key=?2",params![format!("fs:{fs_id}"),format!("task:{task_id}")])?;
        let removed=if fs_id!=0 {
            let n=tx.execute("DELETE FROM files WHERE fs_id=?1 AND state='pending'", [fs_id.to_string()])?;
            tx.execute("DELETE FROM files WHERE fs_id=?1", [fs_id.to_string()])?; n
        } else if let Some(path)=path {
            let n=tx.execute("DELETE FROM files WHERE path=?1 AND state='pending'",[&path])?;
            tx.execute("DELETE FROM files WHERE path=?1",[&path])?; n
        } else {0};
        tx.commit()?; self.pending.fetch_sub(removed,Ordering::SeqCst); Ok(inserted)
    }

    pub fn skip(&self, f:&PendingFile) -> Result<bool> {
        let mut db=self.db.lock(); let tx=db.transaction()?;
        let inserted=tx.execute("INSERT OR IGNORE INTO skipped VALUES(?1,?2,?3)", params![f.relative_path,f.size,chrono::Utc::now().timestamp()])? != 0;
        if inserted {
            tx.execute("INSERT INTO meta VALUES('live_skips',1) ON CONFLICT(key) DO UPDATE SET value=value+1",[])?;
            tx.execute("INSERT INTO meta VALUES('live_skip_bytes',?1) ON CONFLICT(key) DO UPDATE SET value=value+excluded.value",[f.size])?;
        }
        let removed=tx.execute("DELETE FROM files WHERE path=?1 AND state='pending'", [&f.relative_path])?;
        tx.execute("DELETE FROM files WHERE path=?1", [&f.relative_path])?;
        tx.commit()?; self.pending.fetch_sub(removed,Ordering::SeqCst); Ok(inserted)
    }

    pub fn skipped_page(&self, offset:usize, limit:usize) -> Result<Vec<SkippedFile>> {
        let db=self.db.lock(); let mut stmt=db.prepare("SELECT path,size,at FROM skipped ORDER BY path LIMIT ?1 OFFSET ?2")?;
        let rows=stmt.query_map(params![limit as i64,offset as i64], |r| Ok(SkippedFile {relative_path:r.get(0)?,size:r.get(1)?,skipped_at:r.get(2)?}))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn totals(&self) -> Result<(u64,u64,u64,u64)> {
        let db=self.db.lock();
        let (count,bytes):(u64,u64)=db.query_row("SELECT COUNT(*),COALESCE(SUM(size),0) FROM completed",[],|r| Ok((r.get(0)?,r.get(1)?)))?;
        let (skips,skip_bytes):(u64,u64)=db.query_row("SELECT COUNT(*),COALESCE(SUM(size),0) FROM skipped",[],|r| Ok((r.get(0)?,r.get(1)?)))?;
        let floor=|old:&str,new:&str| -> Result<u64> { Ok(db.query_row("SELECT COALESCE(SUM(value),0) FROM meta WHERE key IN (?1,?2)",params![old,new],|r|r.get(0))?) };
        Ok((count.max(floor("legacy_done","live_done")?),bytes.max(floor("legacy_bytes","live_bytes")?),skips.max(floor("legacy_skips","live_skips")?),skip_bytes.max(floor("legacy_skip_bytes","live_skip_bytes")?)))
    }
    pub fn import_history<I>(&self, rows:I) -> Result<()>
    where I: IntoIterator<Item=rusqlite::Result<crate::persistence::history_db::CompletedGroupFile>> {
        let mut db=self.db.lock(); let tx=db.transaction()?;
        let mut removed=0;
        for row in rows {
            let f=row?;
            let key=f.fs_id.filter(|id|*id!=0).map_or_else(||format!("path:{}",f.relative_path),|id|format!("fs:{id}"));
            tx.execute("INSERT INTO completed VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET size=MAX(size,excluded.size)",params![key,f.file_size])?;
            removed += tx.execute("DELETE FROM files WHERE path=?1 AND state='pending'",[&f.relative_path])?;
            tx.execute("DELETE FROM files WHERE path=?1",[&f.relative_path])?;
        }
        tx.commit()?; self.pending.fetch_sub(removed,Ordering::SeqCst); Ok(())
    }
    pub fn task_completed(&self, task_id:&str) -> Result<bool> {
        Ok(self.db.lock().query_row("SELECT EXISTS(SELECT 1 FROM completed_tasks WHERE task_id=?1)",[task_id],|r|r.get(0))?)
    }
    pub fn retry(&self, task_id:&str) -> Result<u32> {
        let db=self.db.lock();
        db.execute("INSERT INTO retries VALUES(?1,1) ON CONFLICT(task_id) DO UPDATE SET attempts=attempts+1",[task_id])?;
        Ok(db.query_row("SELECT attempts FROM retries WHERE task_id=?1",[task_id],|r|r.get(0))?)
    }
    pub fn fail(&self, fs_id:u64, task_id:&str) -> Result<bool> {
        let key=if fs_id==0 {format!("task:{task_id}")} else {format!("fs:{fs_id}")};
        Ok(self.db.lock().execute("INSERT OR IGNORE INTO failures VALUES(?1)",[key])? != 0)
    }
    pub fn clear_failure(&self, fs_id:u64, task_id:&str) -> Result<bool> {
        let key=if fs_id==0 {format!("task:{task_id}")} else {format!("fs:{fs_id}")};
        Ok(self.db.lock().execute("DELETE FROM failures WHERE key=?1",[key])? != 0)
    }
    pub fn failure_count(&self) -> Result<u64> {
        Ok(self.db.lock().query_row("SELECT COUNT(*) FROM failures",[],|r|r.get(0))?)
    }
    pub fn reset_failures(&self) -> Result<()> {
        self.db.lock().execute_batch("DELETE FROM failures; DELETE FROM retries;")?; Ok(())
    }

    /// Old active records are replayed as pending, then reconciled with restored child WALs.
    pub fn recover(&self) -> Result<()> {
        let db=self.db.lock();
        let changed=db.execute("UPDATE files SET state='pending' WHERE state='active'",[])?;
        self.pending.fetch_add(changed,Ordering::SeqCst); Ok(())
    }
    pub fn clear_pending(&self) -> Result<()> { let db=self.db.lock(); db.execute("DELETE FROM files",[])?; self.pending.store(0,Ordering::SeqCst); Ok(()) }
    pub fn cover_paths(&self, paths:&std::collections::HashSet<String>) -> Result<()> {
        let mut db=self.db.lock(); let tx=db.transaction()?;
        let mut changed=0;
        for path in paths { changed += tx.execute("UPDATE files SET state='active' WHERE path=?1 AND state='pending'",[path])?; }
        tx.commit()?; self.pending.fetch_sub(changed,Ordering::SeqCst); Ok(())
    }
    pub fn rename_paths(&self, mappings:&[(String,String)]) -> Result<()> {
        let mut db=self.db.lock(); let tx=db.transaction()?;
        for (old,new) in mappings {
            tx.execute("UPDATE files SET path=?2 || substr(path,length(?1)+1) WHERE path=?1 OR substr(path,1,length(?1)+1)=?1 || '/'",params![old,new])?;
        }
        tx.commit()?; Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    fn file(id:u64, path:&str) -> PendingFile {
        PendingFile { fs_id:id, filename:path.into(), remote_path:format!("/root/{path}"), relative_path:path.into(), size:10 }
    }
    fn open(dir:&Path) -> FolderFileStore {
        let s=FolderFileStore::open(dir,"folder-1",false).unwrap();
        s.migrate(&[],&HashSet::new(),&[],(0,0,0,0)).unwrap(); s
    }
    #[test]
    fn durable_queue_replays_active_and_deduplicates_completions() {
        let dir=tempfile::tempdir().unwrap();
        {
            let s=open(dir.path());
            s.enqueue(&[file(1,"a"),file(2,"b")]).unwrap();
            s.enqueue(&[file(1,"a")]).unwrap();
            assert_eq!(s.pending_count(),2);
            s.mark_active(1,"task-a",Some("a")).unwrap();
            assert_eq!(s.pending_count(),1);
            assert!(s.complete(1,"task-a",10).unwrap());
            assert!(!s.complete(1,"task-a-again",10).unwrap());
            s.mark_active(2,"task-b",Some("b")).unwrap();
        }
        let s=FolderFileStore::open(dir.path(),"folder-1",true).unwrap();
        assert_eq!(s.pending_count(),0);
        s.recover().unwrap();
        assert_eq!(s.pending_count(),1);
        assert_eq!(s.page(1).unwrap()[0].fs_id,2);
        s.enqueue(&[file(1,"a")]).unwrap();
        assert_eq!(s.pending_count(),1);
        assert_eq!(s.totals().unwrap(),(1,10,0,0));
    }
    #[test]
    fn migration_is_idempotent_and_preserves_legacy_count_floor() {
        let dir=tempfile::tempdir().unwrap();
        let s=FolderFileStore::open(dir.path(),"folder-1",false).unwrap();
        let done=HashSet::from([1]);
        let skip=SkippedFile {relative_path:"skip".into(),size:20,skipped_at:1};
        s.migrate(&[file(1,"a"),file(2,"b"),file(3,"skip")],&done,&[skip],(5,50,1,20)).unwrap();
        assert_eq!(s.pending_count(),1);
        s.complete(2,"task-b",10).unwrap();
        s.migrate(&[file(2,"b")],&done,&[],(5,50,1,20)).unwrap();
        assert_eq!(s.pending_count(),0);
        assert_eq!(s.totals().unwrap(),(6,60,1,20));
    }
    #[test]
    fn skipped_and_zero_fs_files_are_independent_and_paged() {
        let dir=tempfile::tempdir().unwrap(); let s=open(dir.path());
        s.enqueue(&[file(0,"a"),file(0,"b"),file(3,"c")]).unwrap();
        s.mark_active(0,"task-a",Some("a")).unwrap();
        assert!(s.complete(0,"task-a",10).unwrap());
        assert!(!s.complete(0,"task-a",10).unwrap());
        assert!(s.is_completed(0,"task-a").unwrap());
        assert!(s.skip(&file(0,"b")).unwrap());
        assert!(!s.skip(&file(0,"b")).unwrap());
        s.skip(&file(3,"c")).unwrap();
        assert_eq!(s.pending_count(),0);
        assert_eq!(s.skipped_page(1,1).unwrap()[0].relative_path,"c");
        s.enqueue(&[file(0,"a"),file(0,"b")]).unwrap();
        assert_eq!(s.pending_count(),0);
        assert_eq!(s.totals().unwrap(),(1,10,2,20));
    }
    #[test]
    fn missing_corrupt_and_uninitialized_required_db_fail_closed() {
        let dir=tempfile::tempdir().unwrap();
        assert!(FolderFileStore::open(dir.path(),"missing",true).is_err());
        let s=FolderFileStore::open(dir.path(),"empty",false).unwrap(); drop(s);
        assert!(FolderFileStore::open(dir.path(),"empty",true).is_err());
        std::fs::write(dir.path().join("folder_files/bad.sqlite"),b"invalid database").unwrap();
        assert!(FolderFileStore::open(dir.path(),"bad",true).is_err());
        assert!(FolderFileStore::open(dir.path(),"../escape",false).is_err());
    }
    #[test]
    fn batch_failure_rolls_back_rows_and_cached_count() {
        let dir=tempfile::tempdir().unwrap();let s=open(dir.path());
        let mut invalid=file(2,"b"); invalid.size=u64::MAX;
        assert!(s.enqueue(&[file(1,"a"),invalid]).is_err());
        assert_eq!(s.pending_count(),0);assert!(s.page(10).unwrap().is_empty());
    }
    #[test]
    fn history_import_is_streamed_and_transactional() {
        let dir=tempfile::tempdir().unwrap();let s=open(dir.path());
        s.enqueue(&[file(1,"a"),file(2,"b")]).unwrap();
        let history=crate::persistence::history_db::CompletedGroupFile {relative_path:"a".into(),fs_id:Some(1),file_size:10};
        assert!(s.import_history(vec![Ok(history.clone()),Err(rusqlite::Error::InvalidQuery)]).is_err());
        assert_eq!(s.pending_count(),2);
        s.import_history(vec![Ok(history)]).unwrap();
        assert_eq!(s.pending_count(),1);assert_eq!(s.totals().unwrap().0,1);
    }
}
