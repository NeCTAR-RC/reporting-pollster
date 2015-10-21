-- So, reworking this to be maintained by a separate program.
--
-- The big change is that the update procedures go away and are replaced by
-- python procedures that will handle all the update processes. That said I
-- may still use a procedure/function to handle updating the metadata table,
-- so that it's a bit simpler.
--
-- In addition to that simple change, we'll be adding a bunch of stuff to the
-- schema that we can't handle well directly in SQL. Hence this schema will
-- become a passive reciever of data rather than actively maintaining the
-- data.

-- drop database if exists reporting;

-- create database reporting;
-- use reporting;

-- metadata - note that this part of the design may change
create table metadata (
        table_name varchar(64), -- this should be an enum, but it's not worth doing that until we know what all the tables are
        last_update timestamp default current_timestamp on update current_timestamp,
        primary key (table_name)
) comment "Database metadata";

-- what else? Also, how to keep this up to date? Triggers, or just enforce it
-- programmatically? Or is that metadata kept in the mysql information_schema
-- somewhere?
--
-- As defined, the timestamp will be updated whenever the matching row is
-- updated, even when the ts column isn't actually set. In addition, we can
-- set the ts value to null, which will update the timestamp to the current
-- value.

-- Physical machines hosting running hypervisor software, aka compute nodes.
--
-- no interaction with other tables at present.
create table hypervisor (
        id int(11) comment "Compute node unique identifier",
        hostname varchar(255) comment "Compute node hostname",
        ip_address varchar(39) comment "Compute node IP address",
        cpus int(11) comment "Number of installed CPU cores",
        memory int(11) comment "Total installed memory in MB",
        local_storage int(11) comment "Total local disk in GB",
        primary key (id),
        key hypervisor_hostname (hostname),
        key hypervisor_ip (ip_address)
) comment "Compute nodes";

-- Projects (otherwise known as tenants) group both users and resources such as instances.
-- Projects are also the finest-grained entity which has resource quotas.
create table project (
        id varchar(36) comment "Unique identifier",
        display_name varchar(64) comment "Human-readable display name",
        enabled boolean comment "If false, the project is not usable by users",
        quota_instances int comment "Maximum concurrent instances",
        quota_vcpus int comment "Maximum concurrent virtual processor cores",
        quota_memory int comment "Maximum memory concurrently allocated in MB",
        quota_volume_total int comment "Maximum total size of storage volumes in GB",
        quota_snapshot int comment "Maximum number of volume snapshots",
        quota_volume_count int comment "Maximum number of concurrently allocated volumes",
        primary key (id)
) comment "Project resource quotas";

-- Users 
create table user (
        id  varchar(64) comment "User unique identifier",
        name varchar(255) comment "User name",
        email varchar(255) comment "User email address",
        default_project varchar(36) comment "User default project",
        enabled boolean,
        primary key (id)
) comment "Users";

-- user roles in projects. Note that this is a many to many relationship:
-- a user can have roles in many projects, and a project may have many users.
create table `role` (
        role varchar(255) comment "Role name",
        user varchar(64) comment "User ID this role is assigned to",
        project varchar(36) comment "Project ID the user is assigned this role in",
--      foreign key role_user_fkey (user) references user(id),
--      foreign key role_project_fkey (project) references project(id)
) comment "User membership of projects, with roles";

-- this one is a real pain, because the flavorid is very similar to the uuid
-- elsewhere, but it's /not/ unique. I didn't want to expose that fact,
-- but there are conflicts otherwise that require me to select only non-deleted
-- records if I stick to the 'uuid' as key.
create table flavour (
        id int(11) comment "Flavour ID",
        uuid varchar(36) comment "Flavour UUID - not unique",
        name varchar(255) comment "Flavour name",
        vcpus int comment "Number of vCPUs",
        memory int comment "Memory in MB",
        root int comment "Size of root disk in GB",
        ephemeral int comment "Size of ephemeral disk in GB",
        public boolean comment "Is this flavour publically available",
        primary key (id),
        key flavour_uuid_key (uuid)
) comment "Types of virtual machine";

-- instances depend on projects and flavours
create table instance (
        project_id varchar(36) comment "Project UUID that owns this instance",
        id varchar(36) comment "Instance UUID",
        name varchar(64) comment "Instance name",
        vcpus int comment "Allocated number of vCPUs",
        memory int comment "Allocated memory in MB",
        root int comment "Size of root disk in GB",
        ephemeral int comment "Size of ephemeral disk in GB",
        flavour int(11) comment "Flavour id used to create instance",
        created_by varchar(36) comment "id of user who created this instance",
        created datetime comment "Time instance was created",
        deleted datetime comment "Time instance was deleted",
        active boolean comment "True if the instance is currently active",
        hypervisor varchar(255) comment "Hypervisor the instance is running on",
        availability_zone varchar(255) comment "Availability zone the instance is running in",
        primary key (id),
        key instance_project_id_key (project_id),
        key instance_hypervisor_key (hypervisor),
        key instance_az_key (availability_zone)
) comment "Virtual machine instances";

-- Storage volumes independent of (but attachable to) virtual machines
-- Volumes (and all the others, in fact) depend on the projects table
create table volume (
        id varchar(36) comment "Volume UUID",
        project_id varchar(36) comment "Project ID that owns this volume",
        display_name varchar(64) comment "Volume display name",
        size int(11) comment "Size in MB",
        created datetime comment "Volume created at",
        deleted datetime comment "Volume deleted at",
        attached boolean comment "Volume attached or not",
        instance_uuid varchar(36) comment "Instance the volume is attached to",
        availability_zone varchar(255) comment "Availability zone the volume exists in",
        primary key (id),
        key volume_project_id_key (project_id),
        key volume_instance_uuid_key (instance_uuid),
        key volume_az_key (availability_zone)
) comment "External storage volumes";

create table image (
        id varchar(36) comment "Image UUID",
        project_id varchar(36) comment "Project ID that owns this image",
        name varchar(255) comment "Image display name",
        size int comment "Size of image in MB",
        -- TODO: It would be nice if status were an enum, and if the view layer could somehow see that.
        status varchar(30) comment "Current status of image",
        public boolean comment "Is this image publically available",
        created datetime comment "Time image was created",
        deleted datetime comment "Time image was deleted",
        primary key (id),
        key image_project_id_key (project_id)
) comment "Operating system images";
