package t::utilBabel;
use t::util;
use Carp;
use Test::More;
use Test::Deep qw(cmp_details deep_diag subbagof);
use List::Util qw(min);
use List::MoreUtils qw(uniq);
use Hash::AutoHash::Args;
# use Hash::AutoHash::MultiValued;
use Exporter();
use strict;
our @ISA=qw(Exporter);

our @EXPORT=
  (@t::util::EXPORT,
   qw(check_object_basics sort_objects
      prep_tabledata load_maptable load_master load_ur 
      select_ur filter_ur count_ur select_ur_sanity 
      check_table check_database_sanity check_maptables_sanity check_masters_sanity 
      cleanup_db cleanup_ur
      cmp_objects cmp_objects_quietly cmp_table cmp_table_quietly
      cmp_op cmp_op_quietly cmp_op_quickly
      check_handcrafted_idtypes check_handcrafted_masters check_handcrafted_maptables
      check_handcrafted_name2idtype check_handcrafted_name2master check_handcrafted_name2maptable
      check_handcrafted_id2object check_handcrafted_id2name check_implicit_masters
      load_handcrafted_maptables load_handcrafted_masters
    ));

sub check_object_basics {
  my($object,$class,$name,$label)=@_;
  report_fail($object,"$label connected object defined") or return 0;
  $object->name;		# touch object in case still Oid
  report_fail(UNIVERSAL::isa($object,$class),"$label: class") or return 0;
  report_fail($object->name eq $name,"$label: name") or return 0;
  return 1;
}
sub check_objects_basics {
  my($objects,$class,$names,$label)=@_;
  my @objects=sort_objects($objects,$label);
  for my $i (0..$#$objects) {
    my $object=$objects->[$i];
    check_object_basics($objects->[$i],$class,$names->[$i],"$label object $i") or return 0;
  }
  return 1;
}
# sort by name.
sub sort_objects {
  my($objects,$label)=@_;
  # hmm.. this doesn't work for Oids. not important anyway, so just bag it
  # TODO: revisit when AutoDB provides public method for fetching Oids.
#   # make sure all objects have names
#   for my $i (0..$#$objects) {
#     my $object=$objects->[$i];
#     report_fail(UNIVERSAL::can($object,'name'),"$label object $i: has name method") 
#       or return ();
#   }
  my @sorted_objects=sort {$a->name cmp $b->name} @$objects;
  wantarray? @sorted_objects: \@sorted_objects;
}
# scrunch whitespace
sub scrunch {
  my($x)=@_;
  $x=~s/\s+/ /g;
  $x=~s/^\s+|\s+$//g;
  $x;
}
sub scrunched_eq {scrunch($_[0]) eq scrunch($_[1]);}

########################################
# these functions deal w/ relational tables

# prepare table data
# data can be 
#   string: one line per row; each row is whitespace-separated values
#   list or ARRAY of strings: each string is row
#   list or ARRAY of ARRAYs: each sub-ARRAY is row
# CAUTION: 2nd & 3rd cases ambiguous: list of 1 ARRAY could fit either case!
sub prep_tabledata {
  # NG 12-08-24: fixed to handle list or ARRAY of ARRAYs as documented
  # my @rows=(@_==1 && !ref $_[0])? split(/\n+/,$_[0]): flatten(@_);
  my @rows=(@_==1 && !ref $_[0])? split(/\n+/,$_[0]): (@_==1)? flatten(@_): @_;
  # clean whitespace and split rows 
  @rows=map {ref($_)? $_: do {s/^\s+|\s+$//g; s/\s+/ /g; [split(' ',$_)]}} @rows;
  # convert NULLS into undefs
  for my $row (@rows) {
    map {$_=undef if 'NULL' eq uc($_)} @$row;
  }
  \@rows;
}
sub load_maptable {
  my($babel,$maptable)=splice(@_,0,2);
  my $data=prep_tabledata(@_);
  ref $maptable or $maptable=$babel->name2maptable($maptable);

  # code adapted from ConnectDots::LoadMapTable Step
  my $tablename=$maptable->tablename;
  my @idtypes=@{$maptable->idtypes};
  my @column_names=map {$_->name} @idtypes;
  my @column_sql_types=map {$_->sql_type} @idtypes;
  my @column_defs=map {$column_names[$_].' '.$column_sql_types[$_]} (0..$#idtypes);
  my @indexes=@column_names;

  # code adapted from MainData::LoadData Step
  my $dbh=$babel->autodb->dbh;
  $dbh->do(qq(DROP TABLE IF EXISTS $tablename));
  my $columns=join(', ',@column_defs);
  $dbh->do(qq(CREATE TABLE $tablename ($columns)));

  # new code: insert data into table
  my @values=map {'('.join(', ',map {$dbh->quote($_)} @$_).')'} @$data;
  my $values=join(",\n",@values);
  $dbh->do(qq(INSERT INTO $tablename VALUES\n$values));

  # code adapted from MainData::LoadData Step
  # put parens around single columns
  my @alters=map {"($_)"} @indexes; # put parens around single columns
  my $alters=join(', ',map {"ADD INDEX $_"} @alters);
  $dbh->do(qq(ALTER TABLE $tablename $alters));
}
sub load_master {
  my($babel,$master)=splice(@_,0,2);
  ref $master or $master=$babel->name2master($master);
  if ($master->implicit) {
  TODO: {
      fail("futile to load data for implicit master. use load_implicit_master instead");
      return;
    }}
  my $data=prep_tabledata(@_);

  # code adapted from ConnectDots::LoadMaster, ConnectDots::LoadImpMaster, MainData::LoadData
  my $tablename=$master->tablename;
  my $idtype=$master->idtype;
  my $column_name=$idtype->name;
  my $column_sql_type=$idtype->sql_type;
  my $column_def="$column_name $column_sql_type";
  # NG 12-11-18: add _X_ column for history
  $column_def.=", _X_$column_name $column_sql_type" if $master->history;
  my $column_list=!$master->history? $column_name: " _X_$column_name, $column_name";

  # NG 12-09-30: no longer get here if master implicit
  # my $query=$master->query;

  my $dbh=$babel->autodb->dbh;
  # NG 12-08-24: moved DROPs out conditionals since master could be table in one babel
  #              and view in another
  $dbh->do(qq(DROP VIEW IF EXISTS $tablename));
  $dbh->do(qq(DROP TABLE IF EXISTS $tablename));
  # NG 12-09-30: no longer get here if master implicit
  # if ($master->view) {
  #   $dbh->do(qq(CREATE VIEW $tablename AS\n$query));
  #   return;
  # }
  my $sql=qq(CREATE TABLE $tablename ($column_def));
  # NG 12-09-30: no longer get here if master implicit
  # $sql.=" AS\n$query" if $master->implicit; # if implicit, load data via query
  $dbh->do($sql);
  # NG 12-09-30: no longer get here if master implicit
  # if (!$master->implicit) {
  # new code: insert data into table
  my @values=map {'('.join(', ',map {$dbh->quote($_)} @$_).')'} @$data;
  my $values=join(",\n",@values);
  $dbh->do(qq(INSERT INTO $tablename ($column_list) VALUES\n$values));
  # }
  # code adapted from MainData::LoadData Step
  $dbh->do(qq(ALTER TABLE $tablename ADD INDEX ($column_name)));
  # NG 12-11-18: add _X_ column for history
  $dbh->do(qq(ALTER TABLE $tablename ADD INDEX ("_X_$column_name"))) if $master->history;

}
# create universal relation (UR)
# algorithm: natual full outer join of all maptables and explicit masters
#            any pre-order traversal of schema graph will work (I think!)
# >>> assume that lexical order of maptables gives a valid pre-order <<<
# sadly, since MyQSL still lacks full outer joins, have to emulate with left/right
# joins plus union. do it step-by-step: I couldn't figure out how to do it in
# one SQL statement...
sub load_ur {
  my($babel,$urname)=@_;
  $urname or $urname='ur';
  # ASSUME that lexical order of maptables gives a valid pre-order
  my @tables=sort {$a->tablename cmp $b->tablename} @{$babel->maptables};
  # add in explicit Masters. order doesn't matter so long as they're last
  push(@tables,grep {$_->explicit} @{$babel->masters});
  # %column2type maps column_names to sql types
  my %column2type;
  my @idtypes=@{$babel->idtypes};
  @column2type{map {$_->name} @idtypes}=map {$_->sql_type} @idtypes;
  my @x_idtypes=grep {$_->history} @idtypes;
  @column2type{map {'_X_'.$_->name} @x_idtypes}=map {$_->sql_type} @x_idtypes;

  my $left=shift @tables;
  while (my $right=shift @tables) {
    my $result_name=@tables? undef: $urname; # final answer is 'ur'
    $left=full_join($babel,$left,$right,$result_name,\%column2type);
  }
  $left;
}
# NG 11-01-21: added 'translate all'
# NG 12-08-22: added 'filters'
# NG 12-09-04: rewrote to do filtering in Perl - seems more robust test strategy
# NG 12-09-21: added support for input_ids=>scalar, filters=>ARRAY,  
#              all semantics of filter=>undef
# NG 12-11-18: added support for histories
# NG 12-11-20: fixed input column for histories: 0th column is '_X_' if input has history
# Ng 12-11-23: added validate
# select data from ur (will actually work for any table)
sub select_ur {
  my $args=new Hash::AutoHash::Args(@_);
  my($babel,$urname,$input_idtype,$input_ids,$output_idtypes,$filters,$validate)=
    @$args{qw(babel urname input_idtype input_ids output_idtypes filters validate)};
  confess "input_idtype must be set. call select_ur_sanity instead" unless $input_idtype;
  # confess "Only one of inputs_ids or input_ids_all may be set" if $input_ids && $input_ids_all;
  $urname or $urname=$args->tablename || 'ur';
  my $input_idtype=ref $input_idtype? $input_idtype->name: $input_idtype;
  if (defined $input_ids) {
    $input_ids=[$input_ids] unless ref $input_ids;
    confess "bad input id: ref or stringified ref"
      if grep {ref($_) || $_=~/ARRAY|HASH/} @$input_ids;
    # NG 12-11-14: drop duplicate input ids so validate won't get extra invalid ids
    $input_ids=[uniq(@$input_ids)];
  }
  my @output_idtypes=map {ref $_? $_->name: $_} @$output_idtypes;
  $filters=filters_array($filters) if ref $filters eq 'ARRAY';
  my @filter_idtypes=keys %$filters;
  
  my $dbh=$babel->autodb->dbh;
  # NG 10-08-25: removed 'uniq' since duplicate columns are supposed to be kept
  # my @columns=uniq grep {length($_)} ($input_idtype,@output_idtypes);
  # NG 12-09-04: include filterut_idtypes so we can do filtering in Perl
  # NG 12-09-04: test for length obsolete, since input_idtype required
  # my @columns=grep {length($_)} ($input_idtype,@filter_idtypes,@output_idtypes);
  # NG 12-11-20: 0th column is '_X_' if input has history
  my @columns=((!_has_history($babel,$input_idtype)? $input_idtype: "_X_$input_idtype"),
	       @filter_idtypes,@output_idtypes);
  # NG 12-11-18: tack on filter history columns
  push(@columns,map {"_X_$_"} grep {_has_history($babel,$_)} @filter_idtypes);
  my $columns=join(', ',@columns);
  my $sql=qq(SELECT DISTINCT $columns FROM $urname WHERE $columns[0] IS NOT NULL);
  my $table=$dbh->selectall_arrayref($sql);
  # hang onto valid input ids if doing validate
  my %valid=map {$_->[0]=>1} @$table if $validate;

  # now do filtering. columns are input, filters, then outputs, finally history columns
  my %name2idx=map {$columns[$_]=>$_} 0..$#columns;
  $table=filter_ur($table,0,$input_ids);
  for(my $j=0; $j<@filter_idtypes && @$table; $j++) {
    my $filter_ids=$filters->{$filter_idtypes[$j]};
    $table=filter_ur($table,$name2idx{"_X_$columns[$j+1]"}||$j+1,$filter_ids);
  }
  # remove filter_idtype columns
  map {splice(@$_,1,@filter_idtypes)} @$table;
  # NG 12-11-18: remove history columns
  map {splice(@$_,1+@output_idtypes)} @$table;
  # remove duplicate rows. dups can arise when filter columns spliced out
  $table=uniq_rows($table);

  # NG 10-11-10: remove rows whose output columns are all NULL, because translate now skips these
  # NG 12-09-04: rewrote loop to one-liner below
  # NG 12-11-23: don't remove NULL rows when validate set
  unless ($validate) {
    @$table=grep {my @row=@$_; grep {defined $_} @row[1..$#row]} @$table if @output_idtypes;
  } else {
    # %id2valid maps input ids to validity
    # %have_id tells which input ids are in result
    # @missing_ids are input ids not in result - some are valid, some not
    $input_ids=[keys %valid] unless $input_ids; # input_ids_all
    my %id2valid=map {$_=>$valid{$_}||0} @$input_ids;
    my %have_id=map {$_->[0]=>1} @$table;
    my @missing_ids=grep {!$have_id{$_}} @$input_ids;
    # existing rows are valid - splice in 'valid' column
    map {splice(@$_,1,0,1)} @$table;
    # add rows for missings ids - some valid, some not
    push(@$table,map {[$_,$id2valid{$_},(undef)x@$output_idtypes]} @missing_ids);
  }
  $table;
}
sub filter_ur {
  my($table,$col,$ids)=@_;
  if (defined $ids) {
    $ids=[$ids] unless ref $ids;
    confess "bad filter id for column $col: ref or stringified ref"
      if grep {ref($_) || $_=~/ARRAY|HASH/} @$ids;
    if (@$ids) {
      my(@table1,@table2);
      my @defined_ids=grep {defined $_} @$ids;
      # NG 12-10-29: changed pattern to match entire field
      my $pattern=join('|',map {"\^$_\$"} @defined_ids);
      $pattern=qr/$pattern/;
      @table1=grep {$_->[$col]=~/$pattern/} @$table if @defined_ids;
      @table2=grep {!defined $_->[$col]} @$table if @defined_ids!=@$ids;
      @$table=(@table1,@table2);
    } else {			# empty list of ids - result empty
      @$table=();
    }
  } else {			# filter=>undef
    @$table=grep {defined $_->[$col]} @$table;
  }
  $table;
}
# remove duplicate rows from table
sub uniq_rows {
  my($rows)=@_;
  my @row_strings=map {join($;,@$_)} @$rows;
  my %seen;
  my $uniq_rows=[];
  for(my $i=0; $i<@$rows; $i++) {
    my $row_string=$row_strings[$i];
    push(@$uniq_rows,$rows->[$i]) unless $seen{$row_string}++;
  }
  $uniq_rows;
}
# process filters ARRAY - a bit hacky 'cuz filter=>undef not same as filter=>[undef]
sub filters_array {
  my @filters=@{$_[0]};
  my(%filters,%filter_undef);
  # code adapted from Hash::AutoHash::MultiValued
  while (@filters>1) { 
    my($key,$value)=splice @filters,0,2; # shift 1st two elements
    if (defined $value || $filter_undef{$key}) { 
      # store value if defined or key has multiple occurrences of undef
      my $list=$filters{$key}||($filters{$key}=[]);
      if (defined $value) {
	push(@$list,$value) unless ref $value;
	push(@$list,@$value) if ref $value;
      }
    } else {
      $filter_undef{$key}++;
    }}
  # add the undefs to %filters
  for my $key (keys %filter_undef) {
    my $list=$filters{$key};
    if (defined $list) {
      push(@$list,undef);
    } else {
      $filters{$key}=undef;
    }
   }
  \%filters;
}

# NG 12-09-04: separated ur sanity tests from real tests
sub select_ur_sanity {
  my $args=new Hash::AutoHash::Args(@_);
  my($babel,$urname,$output_idtypes)=@$args{qw(babel urname output_idtypes)};
  my @output_idtypes=map {ref $_? $_->name: $_} @$output_idtypes;

  my $dbh=$babel->autodb->dbh;
  my $columns=join(', ',@output_idtypes);
  my $sql=qq(SELECT DISTINCT $columns FROM $urname);
  my $table=$dbh->selectall_arrayref($sql);

  # remove NULL rows (probably aren't any)
  @$table=grep {my @row=@$_; grep {defined $_} @row} @$table;
  $table;
}
# NG 12-09-23: added count_ur. simple wrapper around select_ur
sub count_ur {
  my $table=select_ur(@_);
  scalar @$table;
}
# NG 12-11-18: check that table exists and is non-empty
sub check_table {
  my($babel,$table,$label)=@_;
  my $dbh=$babel->autodb->dbh;
  my $ok=1;
  my $sql=qq(SHOW TABLES LIKE '$table');
  my $tables=$dbh->selectcol_arrayref($sql);
  $ok&&=report_fail(!$dbh->err,"$label database query failed: ".$dbh->errstr) or return 0;
  $ok&&=report_fail(scalar @$tables,"$label table $table does not exist") or return 0;
  $ok&&=cmp_quietly($tables,[$table],"$label SHOW TABLES got incorrect result") or return 0;
  my $sql=qq(SELECT COUNT(*) FROM $table);
  my($count)=$dbh->selectrow_array($sql);
  $ok&&=report_fail(!$dbh->err,"$label database query failed: ".$dbh->errstr) or return 0;
  report_fail($count,"$label table $table is empty");
}
# NG 12-11-18: check database for sanity
sub check_database_sanity {
  my($babel,$label,$num_maptables)=@_;
  my $ok=1;
  $ok&&=check_maptables_sanity($babel,"$label check maptables",$num_maptables);
  $ok&&=check_masters_sanity($babel,"$label check masters");
  $ok;
}

# NG 12-11-18: check maptables for sanity
sub check_maptables_sanity {
  my($babel,$label,$num_maptables)=@_;
  my $dbh=$babel->autodb->dbh;
  my $ok=1;
  my @maptables=@{$babel->maptables};
  $ok&&=
    is_quietly($num_maptables,scalar @maptables,"$label BAD NEWS: number of maptables wrong!!")
      or return 0;
  for my $table (map {$_->name} @maptables) {
    $ok&&=check_table($babel,$table,"$label MapTable $table");
  }
  $ok;
}
# NG 12-11-18: check master tables for sanity
sub check_masters_sanity {
  my($babel,$label)=@_;
  my $dbh=$babel->autodb->dbh;
  my $ok=1;
  my @maptables=@{$babel->maptables};
  for my $maptable (@maptables) {
    my $maptable_name=$maptable->name;
    my @idtypes=@{$maptable->idtypes};
    for my $idtype (@idtypes) {
      my $idtype_name=$idtype->name;
      my $master=$idtype->master;
      my $master_name=$master->name;
      $ok&&=is_quietly
	($master_name,"${idtype_name}_master", "$label BAD NEWS: master name wrong!!") 
	  or return 0;
      my $sql=qq(SELECT $idtype_name FROM $maptable_name WHERE $idtype_name NOT IN 
                  (SELECT $idtype_name FROM $master_name));
      my $missing=$dbh->selectcol_arrayref($sql);
      $ok&&=report_fail(!$dbh->err,"$label database query failed: ".$dbh->errstr) or return 0;
      $ok&&=report_fail(@$missing==0,"$label some ids in $maptable_name missing from $master_name; here are a few: ".join(', ',@$missing[0..2])) or return 0;
    }
  }
  $ok;
}

# cmp ARRAYs of Babel component objects (anything with an 'id' method will work)
# like cmp_bag but 
# 1) reports errors the way we want them
# 2) sorts the args to avoid Test::Deep's 'bag' which is ridiculously slow...
sub cmp_objects {
  my($actual,$correct,$label,$file,$line,$limit)=@_;
  my $ok=cmp_objects_quietly($actual,$correct,$label,$file,$line,$limit);
  report_pass($ok,$label);
}
sub cmp_objects_quietly {
  my($actual,$correct,$label,$file,$line)=@_;
  my @actual_sorted=sort {$a->id cmp $b->id} @$actual;
  my @correct_sorted=sort  {$a->id cmp $b->id} @$correct;
  cmp_quietly(\@actual_sorted,\@correct_sorted,$label,$file,$line);
}
# like cmp_bag but 
# 1) reports errors the way we want them
# 2) sorts the args to avoid Test::Deep's 'bag' which is ridiculously slow...
# NG 10-11-08: extend to test limit. CAUTION: limit should be small or TOO SLOW!
sub cmp_table {
  my($actual,$correct,$label,$file,$line,$limit)=@_;
  my $ok=cmp_table_quietly($actual,$correct,$label,$file,$line,$limit);
  report_pass($ok,$label);
}
sub cmp_table_quietly {
  my($actual,$correct,$label,$file,$line,$limit)=@_;
  unless (defined $limit) {
    my @actual_sorted=sort cmp_rows @$actual;
    my @correct_sorted=sort cmp_rows @$correct;
    # my $ok=cmp_quietly($actual,bag(@$correct),$label,$file,$line);
    return cmp_quietly(\@actual_sorted,\@correct_sorted,$label,$file,$line);
  } else {
    my $correct_count=min(scalar(@$correct),$limit);
    report_fail(@$actual==$correct_count,
		"$label: expected $correct_count row(s), got ".scalar @$actual,$file,$line)
      or return 0;
    return cmp_quietly($actual,subbagof(@$correct),$label,$file,$line);
  }
  1;
}
# cmp_op & cmp_op_quietly used for merged translate/count tests
# $actual can be table or count
# $correct always table
# $op is 'translate' or 'count'
sub cmp_op {
  my($actual,$correct,$op,$label,$file,$line,$limit)=@_;
  if ($op eq 'translate') {
    cmp_table($actual,$correct,$label,$file,$line,$limit);
  } elsif ($op eq 'count') {
    $correct=@$correct;
    $correct=min($correct,$limit) if defined $limit;
    my($ok,$details)=cmp_details($actual,$correct);
    report($ok,$label,$file,$line,$details);
  } else {
    confess "Unknow op $op: should be 'translate' or 'count'";
  }
}

sub cmp_op_quietly {
  my($actual,$correct,$op,$label,$file,$line,$limit)=@_;
  if ($op eq 'translate') {
    cmp_table_quietly($actual,$correct,$label,$file,$line,$limit);
  } elsif ($op eq 'count') {
    $correct=@$correct;
    $correct=min($correct,$limit) if defined $limit;
    cmp_quietly($actual,$correct,$label,$file,$line);
  } else {
    confess "Unknow op $op: should be 'translate' or 'count'";
  }
}
# used by big IN tests, because cmp_op way too slow. assumes $correct bigger than $actual
# quiet, even though name doesn't say so
sub cmp_op_quickly {
  my($actual,$correct,$op,$label,$file,$line,$limit)=@_;
  my $correct_count=defined $limit? min(@$correct,$limit): @$correct;
  if ($op eq 'count') {
    return cmp_quietly($actual,$correct_count,$label,$file,$line);
  } elsif ($op eq 'translate') {
    my $actual_count=@$actual;
    my $ok=cmp_quietly($actual_count,$correct_count,$label,$file,$line) or return 0;
    my %correct=map {join($;,@$_)=>1} @$correct;
    my @actual=map {join($;,@$_)} @$actual;
    my @bad=grep {!$correct{$_}} @actual;
    return 1 unless @bad;
    ($file,$line)=called_from($file,$line);
    fail($label);
    diag("from $file line $line") if defined $file;
    diag('actual has ',scalar(@bad),' row(s) that are not in correct',"\n",
	 'sorry I cannot provide details...');
    return 0;
  } else {
    confess "Unknown op $op: should be 'translate' or 'count'" ;
  }
}
# sort subroutine: $a, $b are ARRAYs of strings. should be same lengths. cmp element by element
sub cmp_rows {
  my $ret;
  for (0..$#$a) {
    return $ret if $ret=$a->[$_] cmp $b->[$_];
  }
  # equal up to here. if $b has more, then $a is smaller
  $#$a <=> $#$b;
}
# emulate natural full outer join. return result table
# $result is optional name of result table. if not set, unique name generated
# TODO: add option to delete intermediate tables as we go.
sub full_join {
  my($babel,$left,$right,$resultname,$column2type)=@_;
  my $leftname=$left->tablename;
  my $rightname=$right->tablename;
 # left is usually t::FullOuterJoinTable but can be MapTable or Master
  my @column_names=
    $left->isa('t::FullOuterJoinTable')? @{$left->column_names}: map {$_->name} @{$left->idtypes};
  # right is always MapTable or Master
  push(@column_names,map {$_->name} @{$right->idtypes});
  # NG 12-11:18: added histories
  push(@column_names,'_X_'.$left->idtype->name)
    if $left->isa('Data::Babel::Master') && $left->history;
  push(@column_names,'_X_'.$right->idtype->name)
    if $right->isa('Data::Babel::Master') && $right->history;

  @column_names=uniq(@column_names);
  my @column_defs=map {$_.' '.$column2type->{$_}} @column_names;
  my $column_names=join(', ',@column_names);
  my $column_defs=join(', ',@column_defs);
  
  my $result=new t::FullOuterJoinTable(name=>$resultname,column_names=>\@column_names);
  $resultname=$result->tablename;
  # code adapted from MainData::LoadData Step
  my $dbh=$babel->autodb->dbh;
  $dbh->do(qq(DROP TABLE IF EXISTS $resultname));
  my $column_list=join(', ',@column_defs);
  my $query=qq
    (SELECT $column_names FROM $leftname NATURAL LEFT OUTER JOIN $rightname
     UNION
     SELECT $column_names FROM $leftname NATURAL RIGHT OUTER JOIN $rightname);
  $dbh->do(qq(CREATE TABLE $resultname ($column_list) AS\n$query));
  $result;
}
# drop all tables and views associated with Babel tests
#   arg is generally AutoDB
#   do at start, rather than end, to leave bread crumbs for post-run debugging
sub cleanup_db {
  my($autodb,$keep_ur)=@_;
  my $dbh=$autodb->dbh;
  my @tables=(@{$dbh->selectcol_arrayref(qq(SHOW TABLES LIKE '%maptable%'))},
	      @{$dbh->selectcol_arrayref(qq(SHOW TABLES LIKE '%master%'))});
  map {$dbh->do(qq(DROP TABLE IF EXISTS $_))} @tables;
  map {$dbh->do(qq(DROP VIEW IF EXISTS $_))} @tables;
  cleanup_ur($dbh) unless $keep_ur;
}
# arg is dbh, autodb, or babel. clean up intermediate tables created en route to ur
sub cleanup_ur {t::FullOuterJoinTable->cleanup(@_) }

########################################
# these functions test our hand-crafted Babel & components

sub check_handcrafted_idtypes {
  my($actual,$mature,$label)=@_;
  $label or $label='idtypes'.($mature? ' (mature)': '');
  my $num=4;
  my $class='Data::Babel::IdType';
  report_fail(@$actual==$num,"$label: number of elements") or return 0;
  my @actual=sort_objects($actual,$label) or return 0;
  for my $i (0..$#actual) {
    my $actual=$actual[$i];
    my $suffix='00'.($i+1);
    report_fail(UNIVERSAL::isa($actual,$class),"$label object $i: class") or return 0;
    report_fail($actual->name eq "type_$suffix","$label object $i: name") or return 0;
    report_fail($actual->id eq "idtype:type_$suffix","$label object $i: id") or return 0;
    report_fail($actual->display_name eq "display_name_$suffix","$label object $i: display_name") or return 0;
    report_fail($actual->referent eq "referent_$suffix","$label object $i: referent") or return 0;
    report_fail($actual->defdb eq "defdb_$suffix","$label object $i: defdb") or return 0;
    report_fail($actual->meta eq "meta_$suffix","$label object $i: meta") or return 0;
    report_fail($actual->format eq "format_$suffix","$label object $i: format") or return 0;
    report_fail($actual->sql_type eq "VARCHAR(255)","$label object $i: sql_type") or return 0;
    report_fail(as_bool($actual->internal)==0,"$label object $i: internal") or return 0;
    report_fail(as_bool($actual->external)==1,"$label object $i: external") or return 0;
    if ($mature) {
      check_object_basics($actual->babel,'Data::Babel','test',"$label object $i babel");
      check_object_basics($actual->master,'Data::Babel::Master',
			  "type_${suffix}_master","$label object $i master");
    }
  }
  pass($label);
}

# masters 2&3 are implicit, hence some of their content is special
# NG 10-11-10: implicit Masters now have clauses to exclude NULLs in their queries
sub check_handcrafted_masters {
  my($actual,$mature,$label)=@_;
  $label or $label='masters'.($mature? ' (mature)': '');
  my $num=$mature? 4: 2;
  my $class='Data::Babel::Master';
  report_fail(@$actual==$num,"$label: number of elements") or return 0;
  my @actual=sort_objects($actual,$label) or return 0;
  for my $i (0..$#actual) {
    my $actual=$actual[$i];
    my $suffix='00'.($i+1);
    my $name="type_${suffix}_master";
    my $id="master:$name";
    # masters 2&3 are implicit, hence some of their content is special
    my($inputs,$namespace,$query,$view,$implicit);
    if ($i<2) {
      $inputs="MainData/table_$suffix";
      $namespace="ConnectDots";
      $namespace="ConnectDots";
      $query="SELECT col_$suffix AS type_$suffix FROM table_$suffix";
      $view=0;
      $implicit=0;
    } else {
      $namespace='';		# namespace not in input config file, but hopefully set in output
      $implicit=1;
      if ($i==2) {
	$inputs="ConnectDots/maptable_003 ConnectDots/maptable_002";
	# NG 10-11-10: added clause to exclude NULLs
# 	$query=<<QUERY
# 	SELECT type_003 FROM maptable_003
# 	UNION
# 	SELECT type_003 FROM maptable_002
# QUERY
	$query=<<QUERY
	SELECT type_003 FROM maptable_003 WHERE type_003 IS NOT NULL
	UNION
	SELECT type_003 FROM maptable_002 WHERE type_003 IS NOT NULL
QUERY
  ;
	$view=0;
      } elsif ($i==3) {
	$inputs="ConnectDots/maptable_003";
	# NG 10-11-10: added clause to exclude NULLs
	# $query="SELECT DISTINCT type_004 FROM maptable_003";
	$query="SELECT DISTINCT type_004 FROM maptable_003 WHERE type_004 IS NOT NULL";
	$view=1;      
      }}

    report_fail(UNIVERSAL::isa($actual,$class),"$label object $i: class") or return 0;
    report_fail($actual->name eq $name,"$label object $i: name") or return 0;
    report_fail($actual->id eq $id,"$label object $i: id") or return 0;
    report_fail(scrunched_eq($actual->inputs,$inputs),"$label object $i: inputs") or return 0;
    report_fail(scrunched_eq($actual->namespace,$namespace),"$label object $i: namespace") or return 0;
    report_fail(scrunched_eq($actual->query,$query),"$label object $i: query") or return 0;
    report_fail(as_bool($actual->view)==$view,"$label object $i: view") or return 0;
    report_fail(as_bool($actual->implicit)==$implicit,"$label object $i: implicit") or return 0;
    if ($mature) {
      check_object_basics($actual->babel,'Data::Babel','test',"$label object $i babel");
      check_object_basics($actual->idtype,'Data::Babel::IdType',
			  "type_$suffix","$label object $i idtype");
    }
  }
  pass($label);
}

sub check_handcrafted_maptables {
  my($actual,$mature,$label)=@_;
  $label or $label='maptables'.($mature? ' (mature)': '');
  my $num=3;
  my $class='Data::Babel::MapTable';
  report_fail(@$actual==$num,"$label: number of elements") or return 0;
  my @actual=sort_objects($actual,$label) or return 0;
  for my $i (0..$#actual) {
    my $actual=$actual[$i];
    my $suffix='00'.($i+1);
    my $suffix1='00'.($i+2);
    my $name="maptable_$suffix";
    my $id="maptable:$name";
    my $inputs="MainData/table_$suffix";
    my $query=<<QUERY
SELECT col_$suffix AS type_$suffix, col_$suffix1 AS type_$suffix1
FROM   table_$suffix
QUERY
      ;
    report_fail(UNIVERSAL::isa($actual,$class),"$label object $i: class") or return 0;
    report_fail($actual->name eq $name,"$label object $i: name") or return 0;
    report_fail($actual->id eq $id,"$label object $i: id") or return 0;
    report_fail(scrunched_eq($actual->inputs,$inputs),"$label object $i: inputs") or return 0;
    report_fail(scrunched_eq($actual->namespace,"ConnectDots"),"$label object $i: namespace") or return 0;
    report_fail(scrunched_eq($actual->query,$query),"$label object $i: query") or return 0;
     if ($mature) {
      check_object_basics($actual->babel,'Data::Babel','test',"$label object $i babel");
      check_objects_basics($actual->idtypes,'Data::Babel::IdType',
			  ["type_$suffix","type_$suffix1"],"$label object $i idtypes");
    }
  }
  pass($label);
}

sub check_handcrafted_name2idtype {
  my($babel)=@_;
  my $label='name2idtype';
  my %name2idtype=map {$_->name=>$_} @{$babel->idtypes};
  for my $name (qw(type_001 type_002 type_003 type_004)) {
    my $actual=$babel->name2idtype($name);
    report_fail($actual==$name2idtype{$name},"$label: object $name") or return 0;
  }
  pass($label);
}
sub check_handcrafted_name2master {
  my($babel)=@_;
  my $label='name2master';
  my %name2master=map {$_->name=>$_} @{$babel->masters};
  for my $name (qw(type_001 type_002 type_003 type_004)) {
    my $actual=$babel->name2master($name);
    report_fail($actual==$name2master{$name},"$label: object $name") or return 0;
  }
  pass($label);
}
sub check_handcrafted_name2maptable {
  my($babel)=@_;
  my $label='name2maptable';
  my %name2maptable=map {$_->name=>$_} @{$babel->maptables};
  for my $name (qw(type_001 type_002 type_003 type_004)) {
    my $actual=$babel->name2maptable($name);
    report_fail($actual==$name2maptable{$name},"$label: object $name") or return 0;
  }
  pass($label);
}
sub check_handcrafted_id2object {
  my($babel)=@_;
  my $label='id2object';
  my @objects=(@{$babel->idtypes},@{$babel->masters},@{$babel->maptables});
  my %id2object=map {$_->id=>$_} @objects;
  my @ids=
    (qw(idtype:type_001 idtype:type_002 idtype:type_003 idtype:type_004),
     qw(master:type_001_master master:type_002_master master:type_003_master master:type_004_master),
     qw(maptable:maptable_001 maptable:maptable_002 maptable:maptable_003));
  for my $id (@ids) {
    my $actual=$babel->id2object($id);
    report_fail($actual==$id2object{$id},"$label: object $id") or return 0;
  }
  pass($label);
}
sub check_handcrafted_id2name {
  my($babel)=@_;
  my $label='id2name';
  my @ids=
    (qw(idtype:type_001 idtype:type_002 idtype:type_003 idtype:type_004),
     qw(master:type_001_master master:type_002_master master:type_003_master master:type_004_master),
     qw(maptable:maptable_001 maptable:maptable_002 maptable:maptable_003));
  my @names=
    (qw(type_001 type_002 type_003 type_004),
     qw(type_001_master type_002_master type_003_master type_004_master),
     qw(maptable_001 maptable_002 maptable_003));
  my %id2name=map {$ids[$_]=>$names[$_]} (0..$#ids);
  for my $id (@ids) {
    my $actual=$babel->id2name($id);
    report_fail($actual eq $id2name{$id},"$label: object $id") or return 0;
  }
  pass($label);
}

sub load_handcrafted_maptables {
  my($babel,$data)=@_;
  for my $name (qw(maptable_001 maptable_002 maptable_003)) {
    load_maptable($babel,$name,$data->$name->data);
  }
}
sub load_handcrafted_masters {
  my($babel,$data)=@_;
  # explicit masters
  for my $name (qw(type_001_master type_002_master)) {
    load_master($babel,$name,$data->$name->data);
  }
  # # NG 12-09-27. loop below no subsumed in load_implicit_masters
  # # implicit masters have no data
  # for my $name (qw(type_003_master type_004_master)) {
  #   load_master($babel,$name);
  # }
}
# NG 12-09-27: added load_implicit_masters and test below
# must be called after maptables loaded
sub check_implicit_masters {
  my($babel,$data,$label,$file,$line)=@_;
  my $dbh=$babel->dbh;
  my $ok=1;
  for my $master (grep {$_->implicit} @{$babel->masters}) {
    my $name=$master->name;
    my $correct=prep_tabledata($data->$name->data);
    my $actual=$dbh->selectall_arrayref(qq(SELECT * FROM $name));
    $ok&&=cmp_table_quietly($actual,$correct,"$label: $name",$file,$line);
  }
  report_pass($ok,$label);
}

########################################
# utility functions for history idtypes
# arg is IdType object or name
sub _has_history {
  my($babel,$idtype)=@_;
  ref $idtype or $idtype=$babel->name2idtype($idtype);
  $idtype->history;
}
# sub _history_name {
#   my($babel,$idtype)=@_;
#   ref $idtype and $idtype=$idtype->name;
#   "_X_$idtype";
# }
1;

package t::FullOuterJoinTable;
# simple class to represent intermediate tables used to emulate full outer joins
use strict;
use Carp;
use Class::AutoClass;
use vars qw(@AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS);
use base qw(Class::AutoClass);

@AUTO_ATTRIBUTES=qw(name column_names);
@OTHER_ATTRIBUTES=qw(seqnum);
@CLASS_ATTRIBUTES=qw();
%SYNONYMS=(tablename=>'name',columns=>'column_names');
%DEFAULTS=(column_names=>[]);
Class::AutoClass::declare;

our $seqnum=0;
sub seqnum {shift; @_? $seqnum=$_[0]: $seqnum}

sub _init_self {
  my($self,$class,$args)=@_;
  return unless $class eq __PACKAGE__; # to prevent subclasses from re-running this
  my $name=$self->name || $self->name('fulljoin_'.sprintf('%03d',++$seqnum));
}
sub cleanup {
  my($class,$obj)=@_;
  my $dbh;
  if (ref $obj) {$dbh=$obj->isa('DBI::db')? $obj: $obj->dbh;}
  else {$dbh=Data::Babel->autodb->dbh;}

  # drop all tables that look like our intermediates
  my @tables=@{$dbh->selectcol_arrayref(qq(SHOW TABLES LIKE 'fulljoin_%'))};
  # being a bit paranoid, make sure each table ends with 3 digits
  @tables=grep /\d\d\d$/,@tables;
  map {$dbh->do(qq(DROP TABLE IF EXISTS $_))} @tables;

  # drop ur
  $dbh->do(qq(DROP TABLE IF EXISTS ur));
}
1;
