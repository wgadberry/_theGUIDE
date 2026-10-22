-- =============================================================================
-- T16O Project Initiative — Full DDL
-- Schema prefix : prj_
-- Target        : MySQL 8.0.45
-- Conventions   : no plural table names, soft/hard deletes per object,
--                 separate _ins / _upd procs (existence-check pattern),
--                 prj_sp_<object>_ins | _upd | _get | _del
--                 prj_fn_<object>_get
-- =============================================================================

SET NAMES utf8mb4;
SET time_zone = '+00:00';


-- =============================================================================
-- TABLES
-- =============================================================================

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS prj_analysis_score;
DROP TABLE IF EXISTS prj_snapshot_comparison;
DROP TABLE IF EXISTS prj_forensic_request;
DROP TABLE IF EXISTS prj_annotation;
DROP TABLE IF EXISTS prj_result_snapshot;
DROP TABLE IF EXISTS prj;

SET FOREIGN_KEY_CHECKS = 1;

-- -----------------------------------------------------------------------------
-- prj  (soft delete via archived_at)
-- -----------------------------------------------------------------------------
CREATE TABLE prj (
    id            CHAR(36)      NOT NULL DEFAULT (UUID()),
    user_id       CHAR(36)      NOT NULL,
    name          VARCHAR(120)  NOT NULL,
    description   TEXT          NULL,
    status        VARCHAR(32)   NOT NULL DEFAULT 'active'
                                COMMENT 'active | paused | closed',
    created_at    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                ON UPDATE CURRENT_TIMESTAMP(3),
    archived_at   DATETIME(3)   NULL,

    CONSTRAINT prj_pk          PRIMARY KEY (id),
    CONSTRAINT prj_uq_user_name UNIQUE (user_id, name),

    INDEX prj_idx_user_id      (user_id),
    INDEX prj_idx_status       (status),
    INDEX prj_idx_archived_at  (archived_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Top-level project container per user';


-- -----------------------------------------------------------------------------
-- prj_result_snapshot  (soft delete via archived_at)
-- -----------------------------------------------------------------------------
CREATE TABLE prj_result_snapshot (
    id                  CHAR(36)        NOT NULL DEFAULT (UUID()),
    project_id          CHAR(36)        NOT NULL,
    parent_snapshot_id  CHAR(36)        NULL
                                        COMMENT 'Version chain — points to prior snapshot',
    tool_type           VARCHAR(32)     NOT NULL
                                        COMMENT 'bubblemap | wallet_tx',
    subject_address     VARCHAR(128)    NOT NULL
                                        COMMENT 'Wallet or contract address under investigation',
    version             SMALLINT UNSIGNED NOT NULL DEFAULT 1,
    is_latest           TINYINT      NOT NULL DEFAULT 1,
    result_gz           MEDIUMBLOB      NOT NULL
                                        COMMENT 'gzip-compressed JSON result payload',
    result_hash         CHAR(64)        NOT NULL
                                        COMMENT 'SHA-256 of uncompressed payload — change detection',
    label               VARCHAR(120)    NULL
                                        COMMENT 'Optional user-supplied label for this snapshot',

    -- Lightweight metadata tuple store — avoids decompressing result_gz for scans
    -- Structure: { "summary": [ { "type": "tx", "name": "count", "value": 100 }, ... ] }
    result_metadata     JSON            NULL,

    -- Virtual generated columns for the highest-frequency query paths
    meta_tx_count       BIGINT GENERATED ALWAYS AS (
                            CAST(JSON_UNQUOTE(
                                JSON_EXTRACT(result_metadata,
                                    CONCAT('$.summary[',
                                        IFNULL(
                                            JSON_UNQUOTE(JSON_SEARCH(result_metadata, 'one', 'count',  NULL, '$.summary[*].name')),
                                            'NULL'
                                        ),
                                    '].value'))
                            ) AS SIGNED)
                        ) VIRTUAL,

    captured_at         DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    archived_at         DATETIME(3)     NULL,

    CONSTRAINT prj_rs_pk           PRIMARY KEY (id),
    CONSTRAINT prj_rs_fk_project   FOREIGN KEY (project_id)
                                       REFERENCES prj(id),
    CONSTRAINT prj_rs_fk_parent    FOREIGN KEY (parent_snapshot_id)
                                       REFERENCES prj_result_snapshot(id),
    CONSTRAINT prj_rs_uq_version   UNIQUE (project_id, subject_address, tool_type, version),

    INDEX prj_rs_idx_project       (project_id),
    INDEX prj_rs_idx_subject       (subject_address),
    INDEX prj_rs_idx_tool_type     (tool_type),
    INDEX prj_rs_idx_is_latest     (is_latest),
    INDEX prj_rs_idx_result_hash   (result_hash),
    INDEX prj_rs_idx_archived_at   (archived_at),
    INDEX prj_rs_idx_captured_at   (captured_at),

    -- Multi-valued index on metadata summary type tokens for JSON_CONTAINS lookups
    INDEX prj_rs_idx_meta_type (
        (CAST(result_metadata->'$.summary[*].type' AS CHAR(32) ARRAY))
    )
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Versioned, compressed result payloads from BubbleMap and Wallet Tx tools';


-- -----------------------------------------------------------------------------
-- prj_annotation  (soft delete via deleted_at)
-- -----------------------------------------------------------------------------
CREATE TABLE prj_annotation (
    id              CHAR(36)        NOT NULL DEFAULT (UUID()),
    snapshot_id     CHAR(36)        NOT NULL,
    user_id         CHAR(36)        NOT NULL,
    annotation_type VARCHAR(48)     NOT NULL
                                    COMMENT 'note | flag | link | sybil | bot | forensic',
    body            TEXT            NULL,
    metadata        JSON            NULL
                                    COMMENT 'Type-specific structured payload',
    created_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                    ON UPDATE CURRENT_TIMESTAMP(3),
    deleted_at      DATETIME(3)     NULL,

    CONSTRAINT prj_an_pk              PRIMARY KEY (id),
    CONSTRAINT prj_an_fk_snapshot     FOREIGN KEY (snapshot_id)
                                         REFERENCES prj_result_snapshot(id),

    INDEX prj_an_idx_snapshot_id      (snapshot_id),
    INDEX prj_an_idx_user_id          (user_id),
    INDEX prj_an_idx_annotation_type  (annotation_type),
    INDEX prj_an_idx_deleted_at       (deleted_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='User and system annotations attached to result snapshots';


-- -----------------------------------------------------------------------------
-- prj_forensic_request  (soft delete via archived_at)
-- -----------------------------------------------------------------------------
CREATE TABLE prj_forensic_request (
    id              CHAR(36)        NOT NULL DEFAULT (UUID()),
    project_id      CHAR(36)        NOT NULL,
    snapshot_id     CHAR(36)        NULL
                                    COMMENT 'Optional — may reference a specific snapshot',
    requester_id    CHAR(36)        NOT NULL,
    request_type    VARCHAR(48)     NOT NULL
                                    COMMENT 'internal | legal | compliance | external',
    status          VARCHAR(32)     NOT NULL DEFAULT 'open'
                                    COMMENT 'open | in_progress | resolved | closed',
    notes           TEXT            NULL,
    params          JSON            NULL
                                    COMMENT 'Request-type-specific structured parameters',
    requested_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    resolved_at     DATETIME(3)     NULL,
    archived_at     DATETIME(3)     NULL,

    CONSTRAINT prj_fr_pk             PRIMARY KEY (id),
    CONSTRAINT prj_fr_fk_project     FOREIGN KEY (project_id)
                                        REFERENCES prj(id),
    CONSTRAINT prj_fr_fk_snapshot    FOREIGN KEY (snapshot_id)
                                        REFERENCES prj_result_snapshot(id),

    INDEX prj_fr_idx_project_id      (project_id),
    INDEX prj_fr_idx_snapshot_id     (snapshot_id),
    INDEX prj_fr_idx_requester_id    (requester_id),
    INDEX prj_fr_idx_status          (status),
    INDEX prj_fr_idx_request_type    (request_type),
    INDEX prj_fr_idx_archived_at     (archived_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Formal forensic and compliance requests at project or snapshot level';


-- -----------------------------------------------------------------------------
-- prj_snapshot_comparison  (hard delete — derived, recomputable)
-- -----------------------------------------------------------------------------
CREATE TABLE prj_snapshot_comparison (
    id               CHAR(36)       NOT NULL DEFAULT (UUID()),
    project_id       CHAR(36)       NOT NULL,
    snapshot_a_id    CHAR(36)       NOT NULL,
    snapshot_b_id    CHAR(36)       NOT NULL,
    comparison_type  VARCHAR(48)    NOT NULL
                                    COMMENT 'structural | wallet_overlap | activity_delta | sybil_diff',
    diff_summary     JSON           NULL
                                    COMMENT 'Computed diff payload',
    similarity_score DECIMAL(5,4)   NULL
                                    COMMENT '0.0000–1.0000',
    created_at       DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    CONSTRAINT prj_sc_pk             PRIMARY KEY (id),
    CONSTRAINT prj_sc_fk_project     FOREIGN KEY (project_id)
                                        REFERENCES prj(id),
    CONSTRAINT prj_sc_fk_snapshot_a  FOREIGN KEY (snapshot_a_id)
                                        REFERENCES prj_result_snapshot(id),
    CONSTRAINT prj_sc_fk_snapshot_b  FOREIGN KEY (snapshot_b_id)
                                        REFERENCES prj_result_snapshot(id),
    CONSTRAINT prj_sc_uq_pair        UNIQUE (project_id, snapshot_a_id, snapshot_b_id, comparison_type),

    INDEX prj_sc_idx_project_id      (project_id),
    INDEX prj_sc_idx_snapshot_a      (snapshot_a_id),
    INDEX prj_sc_idx_snapshot_b      (snapshot_b_id),
    INDEX prj_sc_idx_similarity      (similarity_score),
    INDEX prj_sc_idx_comparison_type (comparison_type)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Computed comparisons between two snapshots — hard delete, recomputable';


-- -----------------------------------------------------------------------------
-- prj_analysis_score  (soft delete via archived_at)
-- -----------------------------------------------------------------------------
CREATE TABLE prj_analysis_score (
    id             CHAR(36)       NOT NULL DEFAULT (UUID()),
    snapshot_id    CHAR(36)       NOT NULL,
    score_type     VARCHAR(48)    NOT NULL
                                  COMMENT 'sybil | bot | activity | wash_trade | anomaly',
    score          DECIMAL(6,4)   NOT NULL
                                  COMMENT 'Normalised 0.0000–1.0000',
    confidence     DECIMAL(5,4)   NULL
                                  COMMENT 'Model confidence 0.0000–1.0000',
    evidence       JSON           NULL
                                  COMMENT 'Supporting signals used by the scoring model',
    model_version  VARCHAR(32)    NOT NULL
                                  COMMENT 'Scoring model version — enables re-score comparisons',
    scored_at      DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    archived_at    DATETIME(3)    NULL,

    CONSTRAINT prj_as_pk            PRIMARY KEY (id),
    CONSTRAINT prj_as_fk_snapshot   FOREIGN KEY (snapshot_id)
                                       REFERENCES prj_result_snapshot(id),
    CONSTRAINT prj_as_uq_score      UNIQUE (snapshot_id, score_type, model_version),

    INDEX prj_as_idx_snapshot_id    (snapshot_id),
    INDEX prj_as_idx_score_type     (score_type),
    INDEX prj_as_idx_score          (score),
    INDEX prj_as_idx_model_version  (model_version),
    INDEX prj_as_idx_archived_at    (archived_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Automated detection scores per snapshot — sybil, bot, activity, etc.';


-- =============================================================================
-- STORED PROCEDURES
-- =============================================================================

DELIMITER $$

-- -----------------------------------------------------------------------------
-- prj_sp_prj_ins
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_prj_ins $$
CREATE PROCEDURE prj_sp_prj_ins (
    IN  p_user_id       CHAR(36),
    IN  p_name          VARCHAR(120),
    IN  p_description   TEXT,
    IN  p_status        VARCHAR(32),
    OUT p_new_id        CHAR(36),
    OUT p_result_code   TINYINT,       -- 0 = success, 1 = duplicate name, 99 = error
    OUT p_result_msg    VARCHAR(255)
)
BEGIN
    DECLARE v_exists INT DEFAULT 0;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_result_code = 99;
        SET p_result_msg  = 'Unexpected error in prj_sp_prj_ins';
        ROLLBACK;
    END;

    -- Existence check — no INSERT IGNORE, no auto-increment burn
    SELECT COUNT(1) INTO v_exists
    FROM   prj
    WHERE  user_id = p_user_id
    AND    name    = p_name
    AND    archived_at IS NULL;

    IF v_exists > 0 THEN
        SET p_result_code = 1;
        SET p_result_msg  = 'A project with this name already exists for this user';
        SET p_new_id      = NULL;
    ELSE
        SET p_new_id = UUID();

        INSERT INTO prj (id, user_id, name, description, status)
        VALUES (p_new_id, p_user_id, p_name, p_description,
                IFNULL(p_status, 'active'));

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_prj_upd
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_prj_upd $$
CREATE PROCEDURE prj_sp_prj_upd (
    IN  p_id            CHAR(36),
    IN  p_name          VARCHAR(120),
    IN  p_description   TEXT,
    IN  p_status        VARCHAR(32),
    OUT p_result_code   TINYINT,
    OUT p_result_msg    VARCHAR(255)
)
BEGIN
    DECLARE v_exists      INT DEFAULT 0;
    DECLARE v_name_clash  INT DEFAULT 0;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_result_code = 99;
        SET p_result_msg  = 'Unexpected error in prj_sp_prj_upd';
        ROLLBACK;
    END;

    SELECT COUNT(1) INTO v_exists
    FROM   prj
    WHERE  id = p_id AND archived_at IS NULL;

    IF v_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Project not found or is archived';
    ELSE
        -- Check new name won't clash with another project for same user
        SELECT COUNT(1) INTO v_name_clash
        FROM   prj p2
        JOIN   prj p1 ON p1.id = p_id
        WHERE  p2.user_id      = p1.user_id
        AND    p2.name         = p_name
        AND    p2.id          != p_id
        AND    p2.archived_at IS NULL;

        IF v_name_clash > 0 THEN
            SET p_result_code = 1;
            SET p_result_msg  = 'Another project with this name already exists for this user';
        ELSE
            UPDATE prj
            SET    name        = IFNULL(p_name,        name),
                   description = IFNULL(p_description, description),
                   status      = IFNULL(p_status,      status)
            WHERE  id = p_id;

            SET p_result_code = 0;
            SET p_result_msg  = 'OK';
        END IF;
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_prj_get  (single project or all active for a user)
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_prj_get $$
CREATE PROCEDURE prj_sp_prj_get (
    IN p_id       CHAR(36),   -- NULL = return all active for p_user_id
    IN p_user_id  CHAR(36)
)
BEGIN
    IF p_id IS NOT NULL THEN
        SELECT id, user_id, name, description, status,
               created_at, updated_at, archived_at
        FROM   prj
        WHERE  id = p_id;
    ELSE
        SELECT id, user_id, name, description, status,
               created_at, updated_at, archived_at
        FROM   prj
        WHERE  user_id     = p_user_id
        AND    archived_at IS NULL
        ORDER  BY name;
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_prj_del  (soft delete)
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_prj_del $$
CREATE PROCEDURE prj_sp_prj_del (
    IN  p_id           CHAR(36),
    OUT p_result_code  TINYINT,
    OUT p_result_msg   VARCHAR(255)
)
BEGIN
    DECLARE v_exists INT DEFAULT 0;

    SELECT COUNT(1) INTO v_exists
    FROM   prj
    WHERE  id = p_id AND archived_at IS NULL;

    IF v_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Project not found or already archived';
    ELSE
        UPDATE prj
        SET    archived_at = CURRENT_TIMESTAMP(3),
               status      = 'closed'
        WHERE  id = p_id;

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_result_snapshot_ins
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_result_snapshot_ins $$
CREATE PROCEDURE prj_sp_result_snapshot_ins (
    IN  p_project_id          CHAR(36),
    IN  p_parent_snapshot_id  CHAR(36),
    IN  p_tool_type           VARCHAR(32),
    IN  p_subject_address     VARCHAR(128),
    IN  p_result_gz           MEDIUMBLOB,
    IN  p_result_hash         CHAR(64),
    IN  p_result_metadata     JSON,
    IN  p_label               VARCHAR(120),
    OUT p_new_id              CHAR(36),
    OUT p_version             SMALLINT UNSIGNED,
    OUT p_result_code         TINYINT,
    OUT p_result_msg          VARCHAR(255)
)
BEGIN
    DECLARE v_project_exists  INT DEFAULT 0;
    DECLARE v_hash_clash      INT DEFAULT 0;
    DECLARE v_next_version    SMALLINT UNSIGNED DEFAULT 1;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_result_code = 99;
        SET p_result_msg  = 'Unexpected error in prj_sp_result_snapshot_ins';
        ROLLBACK;
    END;

    START TRANSACTION;

    SELECT COUNT(1) INTO v_project_exists
    FROM   prj WHERE id = p_project_id AND archived_at IS NULL;

    IF v_project_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Parent project not found or is archived';
        SET p_new_id      = NULL;
        ROLLBACK;
    ELSE
        -- Identical payload for same subject/tool = no-op
        SELECT COUNT(1) INTO v_hash_clash
        FROM   prj_result_snapshot
        WHERE  project_id      = p_project_id
        AND    subject_address = p_subject_address
        AND    tool_type       = p_tool_type
        AND    result_hash     = p_result_hash
        AND    archived_at    IS NULL;

        IF v_hash_clash > 0 THEN
            SET p_result_code = 3;
            SET p_result_msg  = 'Identical result hash already exists for this subject and tool — no new snapshot created';
            SET p_new_id      = NULL;
            ROLLBACK;
        ELSE
            -- Determine next version number
            SELECT IFNULL(MAX(version), 0) + 1 INTO v_next_version
            FROM   prj_result_snapshot
            WHERE  project_id      = p_project_id
            AND    subject_address = p_subject_address
            AND    tool_type       = p_tool_type;

            -- Demote prior latest
            UPDATE prj_result_snapshot
            SET    is_latest = 0
            WHERE  project_id      = p_project_id
            AND    subject_address = p_subject_address
            AND    tool_type       = p_tool_type
            AND    is_latest       = 1;

            SET p_new_id = UUID();

            INSERT INTO prj_result_snapshot (
                id, project_id, parent_snapshot_id, tool_type,
                subject_address, version, is_latest,
                result_gz, result_hash, result_metadata, label
            ) VALUES (
                p_new_id, p_project_id, p_parent_snapshot_id, p_tool_type,
                p_subject_address, v_next_version, 1,
                p_result_gz, p_result_hash, p_result_metadata, p_label
            );

            SET p_version     = v_next_version;
            SET p_result_code = 0;
            SET p_result_msg  = 'OK';
            COMMIT;
        END IF;
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_result_snapshot_upd  (label and metadata only — payload is immutable)
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_result_snapshot_upd $$
CREATE PROCEDURE prj_sp_result_snapshot_upd (
    IN  p_id               CHAR(36),
    IN  p_label            VARCHAR(120),
    IN  p_result_metadata  JSON,
    OUT p_result_code      TINYINT,
    OUT p_result_msg       VARCHAR(255)
)
BEGIN
    DECLARE v_exists INT DEFAULT 0;

    SELECT COUNT(1) INTO v_exists
    FROM   prj_result_snapshot
    WHERE  id = p_id AND archived_at IS NULL;

    IF v_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Snapshot not found or is archived';
    ELSE
        UPDATE prj_result_snapshot
        SET    label           = IFNULL(p_label,           label),
               result_metadata = IFNULL(p_result_metadata, result_metadata)
        WHERE  id = p_id;

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_result_snapshot_get
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_result_snapshot_get $$
CREATE PROCEDURE prj_sp_result_snapshot_get (
    IN p_id          CHAR(36),    -- NULL = list for project (no blob)
    IN p_project_id  CHAR(36),
    IN p_latest_only TINYINT   -- 1 = latest per subject/tool only
)
BEGIN
    IF p_id IS NOT NULL THEN
        -- Full row including blob for single-snapshot fetch
        SELECT id, project_id, parent_snapshot_id, tool_type,
               subject_address, version, is_latest,
               result_gz, result_hash, result_metadata, label,
               captured_at, archived_at
        FROM   prj_result_snapshot
        WHERE  id = p_id;
    ELSE
        -- List view — omit blob for performance
        SELECT id, project_id, parent_snapshot_id, tool_type,
               subject_address, version, is_latest,
               result_hash, result_metadata, label,
               captured_at, archived_at
        FROM   prj_result_snapshot
        WHERE  project_id  = p_project_id
        AND    archived_at IS NULL
        AND    (p_latest_only = 0 OR is_latest = 1)
        ORDER  BY subject_address, tool_type, version DESC;
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_result_snapshot_del  (soft delete)
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_result_snapshot_del $$
CREATE PROCEDURE prj_sp_result_snapshot_del (
    IN  p_id           CHAR(36),
    OUT p_result_code  TINYINT,
    OUT p_result_msg   VARCHAR(255)
)
BEGIN
    DECLARE v_exists     INT DEFAULT 0;
    DECLARE v_is_latest  TINYINT;
    DECLARE v_project_id CHAR(36);
    DECLARE v_subject    VARCHAR(128);
    DECLARE v_tool       VARCHAR(32);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_result_code = 99;
        SET p_result_msg  = 'Unexpected error in prj_sp_result_snapshot_del';
        ROLLBACK;
    END;

    START TRANSACTION;

    SELECT COUNT(1), is_latest, project_id, subject_address, tool_type
    INTO   v_exists, v_is_latest, v_project_id, v_subject, v_tool
    FROM   prj_result_snapshot
    WHERE  id = p_id AND archived_at IS NULL;

    IF v_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Snapshot not found or already archived';
        ROLLBACK;
    ELSE
        UPDATE prj_result_snapshot
        SET    archived_at = CURRENT_TIMESTAMP(3),
               is_latest   = 0
        WHERE  id = p_id;

        -- If we just archived the latest, promote the prior version
        IF v_is_latest = 1 THEN
            UPDATE prj_result_snapshot
            SET    is_latest = 1
            WHERE  project_id      = v_project_id
            AND    subject_address = v_subject
            AND    tool_type       = v_tool
            AND    archived_at    IS NULL
            ORDER  BY version DESC
            LIMIT  1;
        END IF;

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
        COMMIT;
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_annotation_ins
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_annotation_ins $$
CREATE PROCEDURE prj_sp_annotation_ins (
    IN  p_snapshot_id      CHAR(36),
    IN  p_user_id          CHAR(36),
    IN  p_annotation_type  VARCHAR(48),
    IN  p_body             TEXT,
    IN  p_metadata         JSON,
    OUT p_new_id           CHAR(36),
    OUT p_result_code      TINYINT,
    OUT p_result_msg       VARCHAR(255)
)
BEGIN
    DECLARE v_snapshot_exists INT DEFAULT 0;

    SELECT COUNT(1) INTO v_snapshot_exists
    FROM   prj_result_snapshot
    WHERE  id = p_snapshot_id AND archived_at IS NULL;

    IF v_snapshot_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Snapshot not found or is archived';
        SET p_new_id      = NULL;
    ELSE
        SET p_new_id = UUID();

        INSERT INTO prj_annotation (
            id, snapshot_id, user_id, annotation_type, body, metadata
        ) VALUES (
            p_new_id, p_snapshot_id, p_user_id, p_annotation_type, p_body, p_metadata
        );

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_annotation_upd
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_annotation_upd $$
CREATE PROCEDURE prj_sp_annotation_upd (
    IN  p_id               CHAR(36),
    IN  p_body             TEXT,
    IN  p_metadata         JSON,
    OUT p_result_code      TINYINT,
    OUT p_result_msg       VARCHAR(255)
)
BEGIN
    DECLARE v_exists INT DEFAULT 0;

    SELECT COUNT(1) INTO v_exists
    FROM   prj_annotation
    WHERE  id = p_id AND deleted_at IS NULL;

    IF v_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Annotation not found or is deleted';
    ELSE
        UPDATE prj_annotation
        SET    body     = IFNULL(p_body,     body),
               metadata = IFNULL(p_metadata, metadata)
        WHERE  id = p_id;

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_annotation_get
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_annotation_get $$
CREATE PROCEDURE prj_sp_annotation_get (
    IN p_id           CHAR(36),   -- NULL = all for snapshot
    IN p_snapshot_id  CHAR(36),
    IN p_type_filter  VARCHAR(48) -- NULL = all types
)
BEGIN
    IF p_id IS NOT NULL THEN
        SELECT id, snapshot_id, user_id, annotation_type,
               body, metadata, created_at, updated_at, deleted_at
        FROM   prj_annotation
        WHERE  id = p_id;
    ELSE
        SELECT id, snapshot_id, user_id, annotation_type,
               body, metadata, created_at, updated_at, deleted_at
        FROM   prj_annotation
        WHERE  snapshot_id      = p_snapshot_id
        AND    deleted_at      IS NULL
        AND    (p_type_filter  IS NULL OR annotation_type = p_type_filter)
        ORDER  BY created_at DESC;
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_annotation_del  (soft delete)
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_annotation_del $$
CREATE PROCEDURE prj_sp_annotation_del (
    IN  p_id           CHAR(36),
    OUT p_result_code  TINYINT,
    OUT p_result_msg   VARCHAR(255)
)
BEGIN
    DECLARE v_exists INT DEFAULT 0;

    SELECT COUNT(1) INTO v_exists
    FROM   prj_annotation
    WHERE  id = p_id AND deleted_at IS NULL;

    IF v_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Annotation not found or already deleted';
    ELSE
        UPDATE prj_annotation
        SET    deleted_at = CURRENT_TIMESTAMP(3)
        WHERE  id = p_id;

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_forensic_request_ins
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_forensic_request_ins $$
CREATE PROCEDURE prj_sp_forensic_request_ins (
    IN  p_project_id    CHAR(36),
    IN  p_snapshot_id   CHAR(36),
    IN  p_requester_id  CHAR(36),
    IN  p_request_type  VARCHAR(48),
    IN  p_notes         TEXT,
    IN  p_params        JSON,
    OUT p_new_id        CHAR(36),
    OUT p_result_code   TINYINT,
    OUT p_result_msg    VARCHAR(255)
)
BEGIN
    DECLARE v_project_exists INT DEFAULT 0;

    SELECT COUNT(1) INTO v_project_exists
    FROM   prj WHERE id = p_project_id AND archived_at IS NULL;

    IF v_project_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Project not found or is archived';
        SET p_new_id      = NULL;
    ELSE
        SET p_new_id = UUID();

        INSERT INTO prj_forensic_request (
            id, project_id, snapshot_id, requester_id,
            request_type, status, notes, params
        ) VALUES (
            p_new_id, p_project_id, p_snapshot_id, p_requester_id,
            p_request_type, 'open', p_notes, p_params
        );

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_forensic_request_upd  (status / notes update)
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_forensic_request_upd $$
CREATE PROCEDURE prj_sp_forensic_request_upd (
    IN  p_id           CHAR(36),
    IN  p_status       VARCHAR(32),
    IN  p_notes        TEXT,
    OUT p_result_code  TINYINT,
    OUT p_result_msg   VARCHAR(255)
)
BEGIN
    DECLARE v_exists INT DEFAULT 0;

    SELECT COUNT(1) INTO v_exists
    FROM   prj_forensic_request
    WHERE  id = p_id AND archived_at IS NULL;

    IF v_exists = 0 THEN
        SET p_result_code = 2;
        SET p_result_msg  = 'Forensic request not found or is archived';
    ELSE
        UPDATE prj_forensic_request
        SET    status      = IFNULL(p_status, status),
               notes       = IFNULL(p_notes,  notes),
               resolved_at = CASE
                               WHEN p_status IN ('resolved','closed')
                               AND  resolved_at IS NULL
                               THEN CURRENT_TIMESTAMP(3)
                               ELSE resolved_at
                             END
        WHERE  id = p_id;

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_forensic_request_get
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_forensic_request_get $$
CREATE PROCEDURE prj_sp_forensic_request_get (
    IN p_id          CHAR(36),   -- NULL = all for project
    IN p_project_id  CHAR(36),
    IN p_status      VARCHAR(32) -- NULL = all statuses
)
BEGIN
    IF p_id IS NOT NULL THEN
        SELECT * FROM prj_forensic_request WHERE id = p_id;
    ELSE
        SELECT *
        FROM   prj_forensic_request
        WHERE  project_id    = p_project_id
        AND    archived_at  IS NULL
        AND    (p_status    IS NULL OR status = p_status)
        ORDER  BY requested_at DESC;
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_snapshot_comparison_ins
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_snapshot_comparison_ins $$
CREATE PROCEDURE prj_sp_snapshot_comparison_ins (
    IN  p_project_id       CHAR(36),
    IN  p_snapshot_a_id    CHAR(36),
    IN  p_snapshot_b_id    CHAR(36),
    IN  p_comparison_type  VARCHAR(48),
    IN  p_diff_summary     JSON,
    IN  p_similarity_score DECIMAL(5,4),
    OUT p_new_id           CHAR(36),
    OUT p_result_code      TINYINT,
    OUT p_result_msg       VARCHAR(255)
)
BEGIN
    DECLARE v_exists INT DEFAULT 0;

    -- Comparison for this exact pair+type may already exist
    SELECT COUNT(1) INTO v_exists
    FROM   prj_snapshot_comparison
    WHERE  project_id      = p_project_id
    AND    snapshot_a_id   = p_snapshot_a_id
    AND    snapshot_b_id   = p_snapshot_b_id
    AND    comparison_type = p_comparison_type;

    IF v_exists > 0 THEN
        SET p_result_code = 1;
        SET p_result_msg  = 'Comparison already exists for this pair and type — use _upd to refresh';
        SET p_new_id      = NULL;
    ELSE
        SET p_new_id = UUID();

        INSERT INTO prj_snapshot_comparison (
            id, project_id, snapshot_a_id, snapshot_b_id,
            comparison_type, diff_summary, similarity_score
        ) VALUES (
            p_new_id, p_project_id, p_snapshot_a_id, p_snapshot_b_id,
            p_comparison_type, p_diff_summary, p_similarity_score
        );

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_snapshot_comparison_get
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_snapshot_comparison_get $$
CREATE PROCEDURE prj_sp_snapshot_comparison_get (
    IN p_id          CHAR(36),   -- NULL = all for project
    IN p_project_id  CHAR(36),
    IN p_type_filter VARCHAR(48)
)
BEGIN
    IF p_id IS NOT NULL THEN
        SELECT * FROM prj_snapshot_comparison WHERE id = p_id;
    ELSE
        SELECT *
        FROM   prj_snapshot_comparison
        WHERE  project_id      = p_project_id
        AND    (p_type_filter IS NULL OR comparison_type = p_type_filter)
        ORDER  BY created_at DESC;
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_analysis_score_ins
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_analysis_score_ins $$
CREATE PROCEDURE prj_sp_analysis_score_ins (
    IN  p_snapshot_id    CHAR(36),
    IN  p_score_type     VARCHAR(48),
    IN  p_score          DECIMAL(6,4),
    IN  p_confidence     DECIMAL(5,4),
    IN  p_evidence       JSON,
    IN  p_model_version  VARCHAR(32),
    OUT p_new_id         CHAR(36),
    OUT p_result_code    TINYINT,
    OUT p_result_msg     VARCHAR(255)
)
BEGIN
    DECLARE v_exists INT DEFAULT 0;

    SELECT COUNT(1) INTO v_exists
    FROM   prj_analysis_score
    WHERE  snapshot_id   = p_snapshot_id
    AND    score_type    = p_score_type
    AND    model_version = p_model_version
    AND    archived_at  IS NULL;

    IF v_exists > 0 THEN
        SET p_result_code = 1;
        SET p_result_msg  = 'Score already exists for this snapshot/type/model — use _upd or archive prior score first';
        SET p_new_id      = NULL;
    ELSE
        SET p_new_id = UUID();

        INSERT INTO prj_analysis_score (
            id, snapshot_id, score_type, score, confidence, evidence, model_version
        ) VALUES (
            p_new_id, p_snapshot_id, p_score_type, p_score, p_confidence, p_evidence, p_model_version
        );

        SET p_result_code = 0;
        SET p_result_msg  = 'OK';
    END IF;
END $$


-- -----------------------------------------------------------------------------
-- prj_sp_analysis_score_get
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS prj_sp_analysis_score_get $$
CREATE PROCEDURE prj_sp_analysis_score_get (
    IN p_id           CHAR(36),   -- NULL = all for snapshot
    IN p_snapshot_id  CHAR(36),
    IN p_score_type   VARCHAR(48) -- NULL = all types
)
BEGIN
    IF p_id IS NOT NULL THEN
        SELECT * FROM prj_analysis_score WHERE id = p_id;
    ELSE
        SELECT *
        FROM   prj_analysis_score
        WHERE  snapshot_id     = p_snapshot_id
        AND    archived_at    IS NULL
        AND    (p_score_type  IS NULL OR score_type = p_score_type)
        ORDER  BY score_type, scored_at DESC;
    END IF;
END $$


DELIMITER ;


-- =============================================================================
-- SCALAR FUNCTIONS
-- =============================================================================

DELIMITER $$

-- -----------------------------------------------------------------------------
-- prj_fn_snapshot_version_get
-- Returns the current latest version number for a subject/tool within a project
-- -----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS prj_fn_snapshot_version_get $$
CREATE FUNCTION prj_fn_snapshot_version_get (
    p_project_id      CHAR(36),
    p_subject_address VARCHAR(128),
    p_tool_type       VARCHAR(32)
)
RETURNS SMALLINT UNSIGNED
READS SQL DATA
DETERMINISTIC
BEGIN
    DECLARE v_version SMALLINT UNSIGNED DEFAULT 0;

    SELECT IFNULL(MAX(version), 0) INTO v_version
    FROM   prj_result_snapshot
    WHERE  project_id      = p_project_id
    AND    subject_address = p_subject_address
    AND    tool_type       = p_tool_type
    AND    archived_at    IS NULL;

    RETURN v_version;
END $$


-- -----------------------------------------------------------------------------
-- prj_fn_prj_is_active_get
-- Returns 1 if project exists and is not archived, 0 otherwise
-- -----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS prj_fn_prj_is_active_get $$
CREATE FUNCTION prj_fn_prj_is_active_get (
    p_id CHAR(36)
)
RETURNS TINYINT
READS SQL DATA
DETERMINISTIC
BEGIN
    DECLARE v_active TINYINT DEFAULT 0;

    SELECT COUNT(1) INTO v_active
    FROM   prj
    WHERE  id = p_id AND archived_at IS NULL;

    RETURN v_active;
END $$


DELIMITER ;

-- =============================================================================
-- END OF DDL
-- =============================================================================
