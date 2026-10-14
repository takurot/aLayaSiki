use std::time::{Duration, SystemTime};
use storage::{
    repo::{RepoError, Repository},
    session::SessionOwner,
};

#[tokio::test]
async fn session_expiry_checks_owner_without_returning_graph_data() {
    let dir = tempfile::tempdir().unwrap();
    let repo = Repository::open(dir.path().join("sessions.wal"))
        .await
        .unwrap();
    let owner = SessionOwner::new("a", "agent");
    let foreign = SessionOwner::new("b", "agent");
    assert_eq!(
        repo.session_expiry_with_owner("missing", Some(&owner))
            .unwrap(),
        None
    );
    let deadline = SystemTime::now() + Duration::from_secs(60);
    {
        let mut session = repo.session_manager.get_or_create("owned");
        session.owner = Some(owner.clone());
        session.expires_at = deadline;
    }
    assert_eq!(
        repo.session_expiry_with_owner("owned", Some(&owner))
            .unwrap(),
        Some(deadline)
    );
    assert!(matches!(
        repo.session_expiry_with_owner("owned", Some(&foreign)),
        Err(RepoError::SessionAccessDenied(_))
    ));
    assert!(matches!(
        repo.session_expiry_with_owner("owned", Some(&SessionOwner::new("a", "other"))),
        Err(RepoError::SessionAccessDenied(_))
    ));
    assert_eq!(
        repo.session_expiry_with_owner("owned", None).unwrap(),
        Some(deadline)
    );
    {
        let mut session = repo.session_manager.get_or_create("unowned");
        session.expires_at = deadline;
    }
    assert!(matches!(
        repo.session_expiry_with_owner("unowned", Some(&owner)),
        Err(RepoError::SessionAccessDenied(_))
    ));
    {
        let mut session = repo.session_manager.get_or_create("owned");
        session.expires_at = SystemTime::UNIX_EPOCH;
    }
    assert_eq!(
        repo.session_expiry_with_owner("owned", Some(&owner))
            .unwrap(),
        None
    );
}
