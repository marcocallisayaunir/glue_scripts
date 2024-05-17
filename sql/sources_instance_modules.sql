CREATE TABLE lms_instances(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    name_instance VARCHAR(55) NOT NULL
);

CREATE TABLE users(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_integration_proeduca VARCHAR(255) NOT NULL UNIQUE,
    id_internal_user INT NOT NULL,
    user_login VARCHAR(255) NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    role_type VARCHAR(50) DEFAULT('student') NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id), -- Foreign Key Constraint
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT (current_date),
    CONSTRAINT unique_id_internal_user UNIQUE (id_internal_user) -- Unique constraint for id_internal_user
);

CREATE TABLE studys(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_integration_study VARCHAR(50) NOT NULL ,
    description VARCHAR(255) NOT NULL,
    id_internal_study INT NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT (current_date),
    CONSTRAINT id_internal_study UNIQUE (id_internal_study) -- Unique constraint for id_internal_study
);
COMMENT ON TABLE studys IS 'This table study = category lms';

CREATE TABLE subjects(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_integration_subject VARCHAR(50) NOT NULL,
    id_internal_study VARCHAR(50) NOT NULL,
    description VARCHAR(255) NOT NULL,
    id_internal_subject INT NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT (current_date),
    CONSTRAINT id_internal_subject UNIQUE (id_internal_subject), -- Unique constraint for id_internal_subject
    FOREIGN KEY (id_internal_study) REFERENCES studys(id_internal_study) -- Foreign Key Constraint
);
COMMENT ON TABLE subjects IS 'This table subject = course lms';

CREATE TABLE groups_manager(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_integration_manager VARCHAR(50) NOT NULL,
    description VARCHAR(255) NOT NULL,
    id_internal_group INT NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id),
    CONSTRAINT id_internal_group UNIQUE (id_internal_group) -- Unique constraint for id_internal_subject
);
COMMENT ON TABLE groups_manager IS 'This table group_manager = groups lms';

CREATE TABLE groups_manager_participants(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_internal_group INT NOT NULL,
    id_internal_user INT NOT NULL,
    id_internal_subject INT NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id),
    FOREIGN KEY (id_internal_group) REFERENCES groups_manager(id_internal_group), -- Foreign Key Constraint
    FOREIGN KEY (id_internal_user) REFERENCES users(id_internal_user), -- Foreign Key Constraint
    FOREIGN KEY (id_internal_subject) REFERENCES subjects(id_internal_subject) -- Foreign Key Constraint
);
COMMENT ON TABLE groups_manager_participants IS 'This table partaker,classmate = groups members lms';
COMMENT ON COLUMN groups_manager_participants.id_internal_group IS 'Its reference id group table lms.';
COMMENT ON COLUMN groups_manager_participants.id_internal_user IS 'Its reference id user table lms.';
COMMENT ON COLUMN groups_manager_participants.id_internal_subject IS 'Its reference id course table lms.';

CREATE TABLE activities(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_internal_subject INT NOT NULL,
    id_internal_activity INT NOT NULL,
    title VARCHAR(50) NOT NULL,
    type_module VARCHAR(50) NOT NULL,
    id_module_internal INT NOT NULL,
    grade_type_activity VARCHAR(50) NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id),
    CONSTRAINT id_internal_activity UNIQUE (id_internal_activity), -- Unique constraint for id_internal_subject
    CONSTRAINT id_module_internal UNIQUE (id_module_internal) -- Unique constraint for id_internal_subject
);
COMMENT ON TABLE activities IS 'This table activity = grade_items table lms';
COMMENT ON COLUMN activities.id_internal_subject IS 'Its reference id course table lms.';
COMMENT ON COLUMN activities.id_internal_activity IS 'Its reference id grade_items table lms.';
COMMENT ON COLUMN activities.type_module IS 'Its reference the module lms.';
COMMENT ON COLUMN activities.id_module_internal IS 'Its reference the id generated module lms.';
COMMENT ON COLUMN activities.grade_type_activity IS 'Its reference whether or not it is scoring.';
COMMENT ON COLUMN activities.id_data_origin IS 'Its reference lms instance.';

CREATE TABLE submissions(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_module_internal INT NOT NULL,
    id_internal_subject INT NOT NULL,
    date_delivery TIMESTAMP NOT NULL,
    id_internal_student INT NOT NULL,
    date_submission TIMESTAMP NOT NULL,
    date_submission_limit TIMESTAMP NOT NULL,
    status_submission VARCHAR(50) NOT NULL,
    FOREIGN KEY (id_module_internal) REFERENCES activities(id_module_internal), -- Foreign Key Constraint
    FOREIGN KEY (id_internal_subject) REFERENCES subjects(id_internal_subject), -- Foreign Key Constraint
    FOREIGN KEY (id_internal_student) REFERENCES users(id_internal_user) -- Foreign Key Constraint
);
COMMENT ON TABLE submissions IS 'This table submissions = assign, assign_submission table lms';
COMMENT ON COLUMN submissions.status_submission IS 'Its reference status assign_submission.';
COMMENT ON COLUMN submissions.id_internal_subject IS 'Its reference id course table lms.';
COMMENT ON COLUMN submissions.id_internal_student IS 'Its reference id users table lms.';

CREATE TABLE test(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_module_internal INT NOT NULL,
    status_test VARCHAR(50) NOT NULL
    FOREIGN KEY (id_module_internal) REFERENCES activities(id_module_internal) -- Foreign Key Constraint
);
COMMENT ON TABLE test IS 'This table submissions = quiz table lms';
COMMENT ON COLUMN test.id_module_internal IS 'Its reference id quiz table lms.';
COMMENT ON COLUMN test.status_test IS 'Its reference timeopen, timeclose table lms.';

CREATE TABLE forums(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_module_internal INT NOT NULL,
    type_forum VARCHAR(50) NOT NULL,
    state_forum VARCHAR(50) NOT NULL
--     FOREIGN KEY (id_module_internal) REFERENCES activities(id_module_internal) -- Foreign Key Constraint
);
COMMENT ON TABLE forums IS 'This table forums = forums table lms';
COMMENT ON COLUMN forums.id_module_internal IS 'Its reference id forum table lms.';
COMMENT ON COLUMN forums.type_forum IS 'Its reference type forum table lms.';
COMMENT ON COLUMN forums.id_module_internal IS 'Its reference state_forum open, close.';

CREATE TABLE grades(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_internal_activity INT NOT NULL,
    id_internal_user_student INT NOT NULL,
    id_internal_user_teacher INT NOT NULL,
    date_grade TIMESTAMP NOT NULL,
    grade_value DECIMAL(10,2) NOT NULL,
    date_grade_limit TIMESTAMP NULL
--     FOREIGN KEY (id_internal_activity) REFERENCES activities(id_internal_activity), -- Foreign Key Constraint
--     FOREIGN KEY (id_internal_user_student) REFERENCES users(id_internal_user), -- Foreign Key Constraint
--     FOREIGN KEY (id_internal_user_teacher) REFERENCES users(id_internal_user) -- Foreign Key Constraint
);
COMMENT ON TABLE grades IS 'This table grades = grades_grade table lms';
COMMENT ON COLUMN grades.id_internal_user_student IS 'Its reference id student to user lms.';
COMMENT ON COLUMN grades.id_internal_user_teacher IS 'Its reference id teacher to user lms.';
COMMENT ON COLUMN grades.date_grade IS 'Its reference date grade the activity.';
COMMENT ON COLUMN grades.grade_value IS 'Its reference grade the activity.';

INSERT INTO lms_instances(name_instance) VALUES
('educacion'),
('humanium'),
('tucampus'),
('decroly'),
('gif'),
('unipro'),
('octavalo'),
('qualentum'),
('preonboarding'),
('internaciones'),
('colombia'),
('mexico'),
('neumann'),
('esit'),
('cunidmad');

---- NOT FOREING KEY

CREATE TABLE lms_instances(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    name_instance VARCHAR(55) NOT NULL
);

CREATE TABLE users(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_integration_proeduca VARCHAR(255) NOT NULL UNIQUE,
    id_internal_user INT NOT NULL,
    user_login VARCHAR(255) NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    role_type VARCHAR(50) DEFAULT('student') NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id), -- Foreign Key Constraint
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT (current_date),
    CONSTRAINT unique_id_internal_user UNIQUE (id_internal_user) -- Unique constraint for id_internal_user
);

CREATE TABLE studys(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_integration_study VARCHAR(50) NOT NULL ,
    description VARCHAR(255) NOT NULL,
    id_internal_study INT NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT (current_date),
    CONSTRAINT id_internal_study UNIQUE (id_internal_study) -- Unique constraint for id_internal_study
);
COMMENT ON TABLE studys IS 'This table study = category lms';

CREATE TABLE subjects(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_integration_subject VARCHAR(50) NOT NULL,
    id_internal_study VARCHAR(50) NOT NULL,
    description VARCHAR(255) NOT NULL,
    id_internal_subject INT NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT (current_date),
    CONSTRAINT id_internal_subject UNIQUE (id_internal_subject) -- Unique constraint for id_internal_subject
);
COMMENT ON TABLE subjects IS 'This table subject = course lms';

CREATE TABLE groups_manager(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_integration_manager VARCHAR(50) NOT NULL,
    description VARCHAR(255) NOT NULL,
    id_internal_group INT NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id),
    CONSTRAINT id_internal_group UNIQUE (id_internal_group) -- Unique constraint for id_internal_subject
);
COMMENT ON TABLE groups_manager IS 'This table group_manager = groups lms';

CREATE TABLE groups_manager_participants(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_internal_group INT NOT NULL,
    id_internal_user INT NOT NULL,
    id_internal_subject INT NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id)
);
COMMENT ON TABLE groups_manager_participants IS 'This table partaker,classmate = groups members lms';
COMMENT ON COLUMN groups_manager_participants.id_internal_group IS 'Its reference id group table lms.';
COMMENT ON COLUMN groups_manager_participants.id_internal_user IS 'Its reference id user table lms.';
COMMENT ON COLUMN groups_manager_participants.id_internal_subject IS 'Its reference id course table lms.';

CREATE TABLE activities(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_internal_subject INT NOT NULL,
    id_internal_activity INT NOT NULL,
    title VARCHAR(50) NOT NULL,
    type_module VARCHAR(50) NOT NULL,
    id_module_internal INT NOT NULL,
    grade_type_activity VARCHAR(50) NOT NULL,
    id_data_origin INT NOT NULL REFERENCES lms_instances(id),
    CONSTRAINT id_internal_activity UNIQUE (id_internal_activity), -- Unique constraint for id_internal_subject
    CONSTRAINT id_module_internal UNIQUE (id_module_internal) -- Unique constraint for id_internal_subject
);
COMMENT ON TABLE activities IS 'This table activity = grade_items table lms';
COMMENT ON COLUMN activities.id_internal_subject IS 'Its reference id course table lms.';
COMMENT ON COLUMN activities.id_internal_activity IS 'Its reference id grade_items table lms.';
COMMENT ON COLUMN activities.type_module IS 'Its reference the module lms.';
COMMENT ON COLUMN activities.id_module_internal IS 'Its reference the id generated module lms.';
COMMENT ON COLUMN activities.grade_type_activity IS 'Its reference whether or not it is scoring.';
COMMENT ON COLUMN activities.id_data_origin IS 'Its reference lms instance.';

CREATE TABLE submissions(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_module_internal INT NOT NULL,
    id_internal_subject INT NOT NULL,
    date_delivery TIMESTAMP NOT NULL,
    id_internal_student INT NOT NULL,
    date_submission TIMESTAMP NOT NULL,
    date_submission_limit TIMESTAMP NOT NULL,
    status_submission VARCHAR(50) NOT NULL
);
COMMENT ON TABLE submissions IS 'This table submissions = assign, assign_submission table lms';
COMMENT ON COLUMN submissions.status_submission IS 'Its reference status assign_submission.';
COMMENT ON COLUMN submissions.id_internal_subject IS 'Its reference id course table lms.';
COMMENT ON COLUMN submissions.id_internal_student IS 'Its reference id users table lms.';

CREATE TABLE test(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_module_internal INT NOT NULL,
    status_test VARCHAR(50) NOT NULL
);
COMMENT ON TABLE test IS 'This table submissions = quiz table lms';
COMMENT ON COLUMN test.id_module_internal IS 'Its reference id quiz table lms.';
COMMENT ON COLUMN test.status_test IS 'Its reference timeopen, timeclose table lms.';

CREATE TABLE forums(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_module_internal INT NOT NULL,
    type_forum VARCHAR(50) NOT NULL,
    state_forum VARCHAR(50) NOT NULL
);
COMMENT ON TABLE forums IS 'This table forums = forums table lms';
COMMENT ON COLUMN forums.id_module_internal IS 'Its reference id forum table lms.';
COMMENT ON COLUMN forums.type_forum IS 'Its reference type forum table lms.';
COMMENT ON COLUMN forums.id_module_internal IS 'Its reference state_forum open, close.';

CREATE TABLE grades(
    id INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    id_internal_activity INT NOT NULL,
    id_internal_user_student INT NOT NULL,
    id_internal_user_teacher INT NOT NULL,
    date_grade TIMESTAMP NOT NULL,
    grade_value DECIMAL(10,2) NOT NULL,
    date_grade_limit TIMESTAMP NULL
);
COMMENT ON TABLE grades IS 'This table grades = grades_grade table lms';
COMMENT ON COLUMN grades.id_internal_user_student IS 'Its reference id student to user lms.';
COMMENT ON COLUMN grades.id_internal_user_teacher IS 'Its reference id teacher to user lms.';
COMMENT ON COLUMN grades.date_grade IS 'Its reference date grade the activity.';
COMMENT ON COLUMN grades.grade_value IS 'Its reference grade the activity.';