package Data::Workflow::Step::database;

#################################################################################
#
# Author:  Nat Goodman
# Created: 11-04-02
# $Id: 
#
# Base class for database Steps.  CANNOT BE EXECUTED DIRECTLY!
# provides basic methods (see below). subclasses
#   bundle methods for common cases
# basic methods
#   create_table
#   create_view
#   drop       drop table or view
#   load_data  add data to table (via LOAD DATA INFILE)
#   insert     add data to table using query
#   create_indexes
#   disable_keys, enable_keys
#   select     retrieve data (via SELECT INTO OUTFILE) - NOT YET IMPLEMENTED   
#
#################################################################################

use strict;
use Carp;
use Data::Workflow::VersionMap;
use Data::Workflow::Util qw(choose_file dezip_file file flatten uniq);
use File::Temp qw(tempfile);
use base qw(Data::Workflow::Step);
use vars qw(@AUTO_ATTRIBUTES @OTHER_ATTRIBUTES);
@AUTO_ATTRIBUTES=qw(_tablename tablename_prefix columns indexes unique_indexes index_all 
		    query view skip);
@OTHER_ATTRIBUTES=qw(tablename);
Class::AutoClass::declare(__PACKAGE__);

sub execute {
  my($self,$pipeline,$mode)=@_;
  confess "Cannot execute 'database' Step directly.  Must use subclasses";
}
# use final component of Step ID if tablename not explicitly set
# prepend tablename_prefix if set
sub tablename {
  my $self=shift;
  my $tablename=$self->_tablename;
  if (@_ || !$tablename) {	# we're setting tablename
    $tablename=$_[0] || file($self->id);
    my $tablename_prefix=$self->tablename_prefix;
    return $self->_tablename($tablename_prefix? join('_',$tablename_prefix,$tablename): $tablename);
  }
  $tablename;
}
sub create_table {
  my($self,$columns,$query,$database_inv,$database_outv)=@_;
  my($log,$tablename)=$self->get(qw(log tablename));
  my $dbh=$query? $self->get_dbh($database_inv): $self->get_dbh($database_outv);
  my $outdb=$database_outv->use_database;
  $tablename="$outdb.$tablename" if $query &&  $database_inv!=$database_outv;
  confess "No column definitions or query provided for table $tablename"
    unless $columns || $query;
  ref $columns and $columns=join(', ',flatten($columns));
  my $sql="CREATE TABLE $tablename";
  $sql.=" ($columns)" if $columns;
  if ($query) {
    printlog $log info=>"creating table $tablename and doing SELECT";
    $sql.=" AS\n$query";
  } else {
    printlog $log info=>"creating table $tablename";
  }
  $dbh->do($sql);
}
sub create_view {
  my($self,$query,$database_inv,$database_outv)=@_;
  my($log,$tablename)=$self->get(qw(log tablename));
  my $dbh=$self->get_dbh($database_inv);
  my $outdb=$database_outv->use_database;
  $tablename="$outdb.$tablename" if $database_inv!=$database_outv;
  confess "No query provided for view $tablename" unless $query;
  my $sql="CREATE VIEW $tablename AS\n$query";
  printlog $log info=>"creating view $tablename";
  $dbh->do($sql);
}
# create table using SQL from schema file
sub create_schema {
  my($self,$sql,$database_outv)=@_;
  my($log,$tablename)=$self->get(qw(log tablename));
  my $dbh=$self->get_dbh($database_outv);
  printlog $log info=>"creating table $tablename";
  # it may be necessary to fix tablename in schema
  $sql=~s/CREATE TABLE \`(\w+)/CREATE TABLE \`$tablename/;
  $dbh->do($sql);
}
sub drop {
  my($self,$database_outv)=@_;
  my($log,$tablename)=$self->get(qw(log tablename));
  my $dbh=$self->get_dbh($database_outv);
  printlog $log info=>"dropping table or view $tablename";
  $dbh->do(qq(DROP TABLE IF EXISTS $tablename));
  $dbh->do(qq(DROP VIEW IF EXISTS $tablename));
}
# @inputs are either path versions or filenames
sub load_data {
  my($self,$skip,$database_outv,@inputs)=@_;
  my($log,$tablename)=$self->get(qw(log tablename));
  my $dbh=$self->get_dbh($database_outv);
  for my $input (@inputs) {
    my $file=ref $input? $input->full_id: $input;
    printlog $log info=>"loading file $file into $tablename";
    # NG 06-06-23: added FIELDS ESCAPED BY ''. turns field escaping OFF. 
    #              default escape character is '\'. leaving this set causes
    #              \'s in data to be swallowed.
    # NG 06-06-27: undid FIELDS ESCAPED BY change because it broke NULL handing
    #              MySQL docs say that literal word NULL should work, but it
    #              doesn't. sigh...
    # DM 08-12-08: removed the LOCAL keyword as it was causing files not to be loaded into MySQL 5.0.67
    # NG 11-02-20: added 'skip' method to skip file header
    # NG 11-04-18: handle zipped files
    my($realfile,$suffix,$stat)=choose_file($file);
    if ($suffix) {		# unzip to temp file
      my $open_arg=dezip_file($realfile);
      open(my $REAL,$open_arg) || confess "Cannot open file $realfile for read: $!";
      my($TEMP,$tempfile)=tempfile();
      print $TEMP (<$REAL>);
      close $TEMP;
      $file=$tempfile;
    }
    my $sql=qq(LOAD DATA LOCAL INFILE '$file' INTO TABLE $tablename);
    $sql.=" IGNORE $skip LINES" if $skip;
    my $rv = $dbh->do($sql);
    if (!$rv or $@) {
      my $err_msg = $@ || $dbh->errstr;
      printlog $log info=>"error loading file $file: $err_msg.";
    }
  }
}
sub insert {
  my($self,$columns,$query,$database_inv,$database_outv)=@_;
  my($log,$tablename)=$self->get(qw(log tablename));
  my $dbh=$self->get_dbh($database_inv);
  my $outdb=$database_outv->use_database;
  $tablename="$outdb.$tablename" if $database_inv!=$database_outv;
  ref $columns and $columns=join(', ',flatten($columns));
  printlog $log info=>"inserting into table $tablename";
  my $sql="INSERT INTO TABLE $tablename";
  $sql.=" ($columns)" if $columns;
  $sql.=" AS\n$query";
  $dbh->do($sql);
}
# NG 11-05-20: added index_all. boolean. if set, create indexes for all columns
sub create_indexes {
  my($self,$indexes,$unique_indexes,$index_all,$database_outv)=@_;
  my($log,$tablename)=$self->get(qw(log tablename));
  my $dbh=$self->get_dbh($database_outv);
  
  # NG 10-08-12: moved check for empty index list before 'printlog' because misleading
  #              to say 'creating indexes for...' when there are no indexes
  # NG 08-05-05: changed to allow empty index list
  # confess 'subclass '.(ref $self).' did not provide index definintions.' unless $indexes;
  return unless $indexes || $unique_indexes || $index_all;

  printlog $log info=>"creating indexes for $tablename";
  my $sql="ALTER TABLE $tablename ";
  # TODO: do a better job of parsing indexes. 
  #       multi-column indexes can easily fool the split patterns
  my @unique_indexes=ref $unique_indexes? flatten($unique_indexes): split(/\s+/,$unique_indexes);
  my @indexes=ref $indexes? flatten($indexes): split(/\s+/,$indexes);
  if ($index_all) {
    my $sql=qq(SHOW COLUMNS FROM $tablename);
    my @columns=@{$dbh->selectcol_arrayref($sql)};
    push(@indexes,@columns);
    @indexes=uniq @indexes;
  }
  # as a convenience, put parens around single columns
  @unique_indexes=map {/^\w+$/? "($_)": $_} @unique_indexes;
  @indexes=map {/^\w+$/? "($_)": $_} @indexes;
  my $alters=join(', ',map({"ADD UNIQUE $_"} @unique_indexes),map({"ADD INDEX $_"} @indexes));
  my $sql=qq(ALTER TABLE $tablename $alters);
  $dbh->do($sql) if $alters;
}
sub disable_keys {
  my($self,$database_outv)=@_;
  my($log,$tablename)=$self->get(qw(log tablename));
  my $dbh=$self->get_dbh($database_outv);
  printlog $log info=>"disabling indexes for $tablename";
  my $sql="ALTER TABLE $tablename DISABLE KEYS";
  $dbh->do($sql);
}
sub enable_keys {
  my($self,$database_outv)=@_;
  my($log,$tablename)=$self->get(qw(log tablename));
  my $dbh=$self->get_dbh($database_outv);
  printlog $log info=>"enabling indexes for $tablename";
  my $sql="ALTER TABLE $tablename ENABLE KEYS";
  $dbh->do($sql);
}

1;
