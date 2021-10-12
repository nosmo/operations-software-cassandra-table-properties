CREATE ROLE itest_user WITH password = 'itest_user' AND login=true;
GRANT ALL ON "local_group_default_T_pageviews_per_project_v2".meta to 'itest_user';
GRANT DROP ON "local_group_default_T_pageviews_per_project_v2".data to 'itest_user';
CREATE ROLE itest_role;
GRANT ALTER ON "local_group_default_T_pageviews_per_project_v2".data to 'itest_role';
GRANT itest_role TO itest_user;
/* GRANT SELECT ON "local_group_default_T_pageviews_per_project_v2".data to 'itest_role'; */
