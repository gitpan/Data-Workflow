package Data::Workflow;
our $VERSION='0.10_01';
$VERSION=eval $VERSION;         # I think this is the accepted idiom..
#################################################################################
#
# Author:  Nat Goodman
# Created: 12-12-15
# $Id: 
#
# Copyright 2012 Institute for Systems Biology
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of either: the GNU General Public License as published
# by the Free Software Foundation; or the Artistic License.
#
# See http://dev.perl.org/licenses/ for more information.
#
#################################################################################
use strict;
use Class::AutoClass;
use Carp;
use Exporter qw(import);
use Text::Abbrev;
use Set::Scalar;
use Template;
use Config::IniFiles;
use Graph::Directed;
use Data::Workflow::Log;
use Data::Workflow::Util 
  qw(flatten group parse_list parse_hashl set_defaults first uniq catname splitname);
use Data::Workflow::ResourcePool;
use Data::Workflow::VersionMap;
use Data::Workflow::BFSort;
use Data::Workflow::GraphAlgorithms; # mixin
use Data::Dumper;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Class::AutoClass Data::Workflow::GraphAlgorithms);

# NG 11-01-05: _id2resource no longer used. resource methods now delegate to pool
#              why wasn't it done this way before?? hope I don't find out the hard way:)
@AUTO_ATTRIBUTES=
  qw(id is_prepared create_flag
     _resource_pool _namespaces _version _preprocessors _disease_id _from _prev_db _log
     _id2step _id2resource _id2preprocessor _graph
     verbose);
@OTHER_ATTRIBUTES=qw(id2step id2resource id2preprocessor graph autodb log);
%SYNONYMS=();
%DEFAULTS=(id2step=>{},id2preprocessor=>{},_resource_pool=>'Default ResourcePool');
%AUTODB=
  (-collection=>'Workflow',
   -keys=>qq(id string),
   -transients=>qq(is_prepared create_flag _graph _log
		   verbose));
Class::AutoClass::declare(__PACKAGE__);

our @LITERALS=qw(__SECTION__ __STEP__);
our @PIPE_PARAMS=qw(pipeline resource_pool version);
our @SECT_PARAMS=qw(step_id inputs outputs members format create_paths default_output
		    namespace input_prefix output_prefix step_prefix type type_prefix
		    cat_type_step_prefixes compat_io_prefixes
		    make_namespace);
our %SECT_DEFAULTS=(format=>'multiple',create_paths=>1,
		    step_id=>'__STEP__',output_prefix=>'__STEP__',
		    cat_type_step_prefixes=>0,compat_io_prefixes=>1);
our %FORMATS=(m=>'multiple step',abbrev ('multiple step','members step','single step'));
use constant {MULTIPLE_FORMAT=>'m', SINGLE_FORMAT=>'s'};
our %SHORT_FORMATS=('multiple step'=>MULTIPLE_FORMAT,'members step'=>MULTIPLE_FORMAT,
		    'single step'=>SINGLE_FORMAT);
# NG 10-11-24: added grep
# NG 10-12-22: added untar
# NG 11-01-06: added touch
our @SYSTEM_STEPS=qw(copy cat grep perl touch untar 
		     database::create database::create_schema database::append);
our %SYSTEM_STEPS; @SYSTEM_STEPS{@SYSTEM_STEPS}=@SYSTEM_STEPS;
our $SYSTEM_TYPE_PREFIX='Data::Workflow::Step';	# base of Step class hierarchy
use constant {EXECUTE_OOD=>1, EXECUTE_FORCE=>2, EXECUTE_SHOW=>3};
our %LONG_EMODES=(o=>'ood',abbrev(qw(out_of_date ood old EXECUTE_OOD force all EXECUTE_FORCE 
				     show EXECUTE_SHOW)));
our %EXECUTE_MODES=('out_of_date'=>EXECUTE_OOD,ood=>EXECUTE_OOD,old=>EXECUTE_OOD,
		    EXECUTE_OOD=>EXECUTE_OOD,
		    force=>EXECUTE_FORCE,all=>EXECUTE_FORCE,EXECUTE_FORCE=>EXECUTE_FORCE,
		    show=>EXECUTE_SHOW,EXECUTE_SHOW=>EXECUTE_SHOW);
our @EXPORT=qw(MULTIPLE_FORMAT SINGLE_FORMAT EXECUTE_OOD EXECUTE_FORCE EXECUTE_SHOW);

sub _init_self {
  my ($self, $class, $args) = @_;
  return unless $class eq __PACKAGE__;
  my $old_self;
  my $autodb=$self->autodb;
  # fetch object from database if possible
  my $id=$self->id;
  if ($id && $autodb->is_connected) {
    ($old_self)=$autodb->Class::AutoDB::Database::get(-collection=>'Workflow',-id=>$id);
    if ($old_self) {
      # merge old & new attributes
      # TBD: dependencies, namespace, ...
      # maybe better to do it all via $args, below
      $old_self->log($self->log);
      $old_self->verbose($self->verbose);
      $self=$self->{__OVERRIDE__}=$old_self;
    }}
  # Do processing for arguments. 
  # NG 10-07-30: update to Hash::AutoHash::Args
  my($resource_pool,$version,$preprocessors,$dependencies,$namespaces,$from,$disease_id,$execute,$steps,$sources,$targets,$prev_db)=
#     $args->get_args 
#       qw(resource_pool version preprocessors dependencies namespaces from disease_id execute steps sources targets prev_db);
    @$args
      {qw(resource_pool version preprocessors dependencies namespaces from disease_id execute steps sources targets prev_db)};
  $self->disease_id($disease_id);
  $self->log(null Data::Workflow::Log) unless $self->log;
  # make resource pool unless object already has one
  $self->resource_pool($resource_pool) unless ref $self->resource_pool;
  $self->version($version,$from) if $version;
  $self->namespaces($namespaces) if $namespaces;
  $self->preprocessors($preprocessors) if $preprocessors;
  $self->dependencies($dependencies) if $dependencies;
  $self->prepare;
 # store pipeline if database connected and something has changed
  $self->put if ($autodb && $autodb->is_connected && 
		 (!$old_self || $resource_pool || $version || $namespaces || $dependencies));
  # execute if requested
  if ($execute || $steps || $sources || $targets) {
    my @args=flatten($execute);
    @args=(-mode=>$args[0]) if @args==1 && !ref $args;
    push(@args,(steps=>$steps)) if $steps;
    push(@args,(sources=>$sources)) if $sources;
    push(@args,(targets=>$targets)) if $targets;
    $self->execute(@args);
  }
}
sub dependencies {
  my $self=shift;
  my @sources=@_? do {$self->is_prepared(0); parse_list(@_)}: ();
  return unless @sources;
  # NG 11-01-05: since AutoDB now has 'del' method, use it!
  $self->autodb->del($self->steps);
  $self->id2step({});
  # NG 11-01-05: id2resource now delegated to pool
  # $self->id2resource({});
  my $id2preprocessor=$self->id2preprocessor;
  my $pattern='('.join('|',keys %$id2preprocessor).')';
  for my $source (@sources) {
    my $pre_out;		# output of preprocessor
    my($pre_key)=$source=~/$pattern/;
    if ($pre_key) { # source has a preprocessor
      my $preprocessor=$id2preprocessor->{$pre_key};
      my($pre_class,$params)=@$preprocessor;
#      $params='('."$params,-in_file=>\'$source\'".')';
      $params='('.(length($params)?"$params,":'')."-in_file=>\'$source\'".')';
      eval "use $pre_class";
      if ($@) {			# 'use' failed
	confess "Unkown preprocessor type $pre_class" if $@=~/^Can\'t locate $pre_class/;
	confess $@;
      }
      my $pre_obj=eval "new $pre_class $params";
      if ($@) {			# 'new' failed
	confess $@;
      }
      $pre_out=$pre_obj->process;
    } else {			# for now, TT is default. expect this to go away
      my $template = new Template
	(RELATIVE => 1,
	 ABSOLUTE => 1,
	 INTERPOLATE=>1,		# allow 'naked' use of $ variables
	 EVAL_PERL=>1,		# use of [% PERL %] blocks
	);
      my($tt_out,$cfg);
      # NG 11-01-07: coded adapted from Babel
      # stuff useful environment variables and dbh into stash. USER now. more later maybe
      my $stash={};
      my @envs=qw(USER); @$stash{@envs}=@ENV{@envs};
      $stash->{DBH}=$self->dbh;
      $template->process($source,$stash,\$tt_out) || 
	confess "Template::process failed: ".$template->error();
      print "========== Here is \$tt_out ===========\n",$tt_out,"\n====================\n" 
	if $self->verbose;
      $pre_out=\$tt_out;
    }
    my $cfg;
    if ('SCALAR' eq ref $pre_out) { # ref to string. Config::IniFiles can't swallow this
      open(INI,'<',$pre_out) || confess "Cannot tie preprocessor output string: $!";
      $cfg=new Config::IniFiles(-file=>*INI);
      confess "Confg::IniFiles errors: @Config::IniFiles::errors" unless $cfg;
    } else {
      $cfg=new Config::IniFiles(-file=>$pre_out);
      confess "Confg::IniFiles errors: @Config::IniFiles::errors" unless $cfg;
    }
    # set up defaults from GLOBAL section
    # NG 08-03-14: changed HASH refs to hashes to make code easier to read
    my(%pipe_params,%g_sect_params,%g_step_params);
    set_defaults(%g_sect_params,%SECT_DEFAULTS);

    my $section='GLOBAL';
#    my $type=$cfg->val($section,'type'); # need type to find step params
    for my $param ($cfg->Parameters($section)) {
      my $value=$cfg->val($section,$param);
      if (is_pipe_param($param)) {
	$pipe_params{$param}=$value;
      } elsif (is_sect_param($param)) {
	$g_sect_params{$param}=$value;
      } else {
	$g_step_params{$param}=$value;
      }
    }
    my $ambiguous=
      (new Set::Scalar(keys %g_sect_params) + new Set::Scalar(keys %pipe_params))
	* new Set::Scalar(keys %g_step_params);
    confess "Section [$section] has ambiguous parameter(s) ".join(', ',$ambiguous->members)
      unless $ambiguous->is_empty;
    
    my @sections=$cfg->Sections;
    for my $section (@sections) {
      next if $section eq 'GLOBAL'; # GLOBAL handled above
      
      # NG 08-03-14: added literals, __SECTION__, __STEP__
      # NG 08-03-14: changed HASH refs to hashes to make code easier to read
#      my($literals,$sect_params,$other_params,$step_params,$member_params)=({},{},{},{},{});
      my(%literals,%sect_params,%other_params,%step_params,%member_params);
      my($member_inputs,$general_inputs,$member_outputs,$general_outputs)=({},{},{},{});
      # collect section parameters and others
      for my $param ($cfg->Parameters($section)) {
	my $value=$cfg->val($section,$param);
	$value=~s/\s*$//;	# strip trailing space. TODO: move to IniFiles subclass
	if (is_sect_param($param)) {
	  $sect_params{$param}=$value;
	} else {
	  $other_params{$param}=$value;
	}
      }
      set_defaults(%sect_params,%g_sect_params);
      set_defaults(%step_params,%g_step_params);
      # NG 08-03-14: 'step_prefix' more natural term than 'namespace' for text attached to the
      #              front of the step_id. defaults to namespace
      $sect_params{step_prefix} or $sect_params{step_prefix}=$sect_params{namespace};
      # NG 08-03-14: __STEP__ expanded to correct value later
      # NG 08-03-17: default set in %SECT_DEFAULTS
#      $sect_params{step_id}='__STEP__';
      # NG 08-03-11: very convenient to let type default to section name
      #      confess "Section [$section] has no type and none is defined in [GLOBAL]" unless $type;
      my $type=$sect_params{type};
      my $type_prefix;
      if ($SYSTEM_STEPS{$type}) { # builtin types live in a standard place
	$type_prefix=$sect_params{type_prefix}=$SYSTEM_TYPE_PREFIX;
      } else {
	$type_prefix=$sect_params{type_prefix};
	$type_prefix=~s/::$//;
	$type or $type=$sect_params{type}=path2class($section);
	if ($sect_params{cat_type_step_prefixes}) {
	  my $step_prefix=$sect_params{step_prefix};
	  $type_prefix.='::'.path2class($step_prefix) if $step_prefix;
	}}
      $sect_params{step_class}=$type_prefix? $type_prefix.'::'.$type: $type;

      # NG 08-03-14: 'output_prefix' more natural term than 'namespace' for text attached to the
      #              front of each output. defaults to __STEP__
      $sect_params{output_prefix} or $sect_params{output_prefix}='__STEP__'; # expanded later
      # NG 08-03-14: added default_output (instead of using step_id) so it can be
      #              prepended with the correct prefix
      $sect_params{default_output} or $sect_params{default_output}=$sect_params{output_prefix};
      # default set in %SECT_DEFAULTS, but has to be converted to canonical form
      $sect_params{format}=$SHORT_FORMATS{$FORMATS{$sect_params{format}}};

      # NG 08-03-14: set and interpolate literals
      # NG 11-01-24: added __NAMESPACE__
      $literals{__SECTION__}=$section;
      $literals{__STEP__}=prepend($sect_params{step_prefix},$section);
      $literals{__NAMESPACE__}=$sect_params{namespace};
#     interp_literals(\%pipe_params,\%literals);  # NOP now
      interp_literals(\%sect_params,\%literals);
      interp_literals(\%step_params,\%literals);
      interp_literals(\%other_params,\%literals);

      # convert text lists to Perl ARRAY refs and prepend prefixes
      my($inputs,$outputs)=@sect_params{qw(inputs outputs)};
      my @inputs=split(/\s+/,$inputs);
      my @outputs=split(/\s+/,$outputs);
      if ($sect_params{compat_io_prefixes}) { # preseve compatibilty with old prefix behavior
	# TODO: elimimate need for this case by fixing conf files
	@inputs=prepend_compat($self,$sect_params{input_prefix},@inputs);
	@outputs=prepend_compat($self,$sect_params{output_prefix},@outputs);
      } else {
	@inputs=prepend($sect_params{input_prefix},@inputs);
	@outputs=prepend($sect_params{output_prefix},@outputs);
      }

      # TODO: multiple format deprecated
      my $format=$sect_params{format};
      if ($format eq MULTIPLE_FORMAT) {
	my $members=$sect_params{members};
	my %members;
	if (defined $members) {	          # easy case: get members from members param
	  $members=parse_members($members);
	  if ('ARRAY' eq ref $members) {  # ARRAY of member names
	    @members{@$members}=@$members;
	  } else {		          # HASH of member=>value
	    %members=%$members;
	    %member_params=%members;
	  }
	} else  {			   # magic case: get members from inputs,outputs,params
	  my($member_inputs,$general_inputs)=member2resources(\@inputs,\%other_params);
	  my($member_outputs,$general_outputs)=member2resources(\@outputs,\%other_params);
	  @members{keys %$member_inputs,keys %$member_outputs}=
	    (keys %$member_inputs,keys %$member_outputs);
	}
	# now re-allocate other_params into step_params and member_params
	while(my($param,$value)=each %other_params) {
	  if (exists $members{$param}) {
	    $member_params{$param}=$value;
	  } else {
	    $step_params{$param}=$value;
	  }
	}
	($member_inputs,$general_inputs)=member2resources(\@inputs,\%member_params);
	($member_outputs,$general_outputs)=member2resources(\@outputs,\%member_params);
	# fall back to single format if no members defined
	$format=SINGLE_FORMAT unless keys %member_params;
      } else {			# all non_section, non_pipeline params are step_params
	@step_params{keys %other_params}=values %other_params;
      }
      my $ambiguous=
	new Set::Scalar(keys %sect_params) * new Set::Scalar(keys %step_params);
      confess "Section [$section] has ambiguous parameter(s) ".join(', ',$ambiguous->members)
	unless $ambiguous->is_empty;

      # NG 11-01-05: added make_namespace. usually defined in GLOBAL value can be 
      #   1) type, eg, 'uri' or 'GDxBase::Workflow::Namespace::IxnSource', or 
      #   2) hash of parameter=>type, eg, homepage=>GDxBase::Workflow::Namespace::IxnSource
      if ($sect_params{make_namespace}) {
	my $namespace_type=parse_make_namespace($sect_params{make_namespace},%step_params);
	$self->make_namespace($namespace_type,$section,%step_params) 
	  if $namespace_type;
      }

      # copy into step_params certain section params that are processed by step
      # for now, just create_paths
      my @special=qw(create_paths);
      @step_params{@special}=@sect_params{@special};
      # NG 10-08-01: also pass section name to Step
      $step_params{section_id}=$section unless exists $step_params{section_id};

      if ($format eq SINGLE_FORMAT) {
	# just have to make the Step
	# NG 08-03-14: moved step_id calculation earlier
	@outputs=($sect_params{default_output}) unless @outputs;
	$self->new_step(@sect_params{qw(step_class type step_id)},$format,
			\@inputs,\@outputs,undef,%step_params);
      } else {
	# group inputs and outputs by member
	#my($member_inputs,$general_inputs)=member2resources($inputs,$member_params);
	#my($member_outputs,$general_outputs)=member2resources($outputs,$member_params);

	# now make the Steps
	# NG 08-03-14: step_id processing done earlier. now includes prefix
	for my $member (keys %member_params) {
	  my $step_id=catname($sect_params{step_id},$member);
	  #	confess "Section [$section] has duplicate step $step_id" 
	  #	  if exists $self->id2step->{$step_id};
	  my $inputs=[@$general_inputs,@{$member_inputs->{$member} || []}];
	  my $outputs=[@$general_outputs,@{$member_outputs->{$member} || []}];
	  $outputs=[$sect_params{default_output}.'/'.$member] unless @$outputs;
	  my $main_param=$member_params{$member};
	  $self->new_step(@sect_params{qw(step_class type)},$step_id,$format,
			  $inputs,$outputs,$main_param,%step_params);
	}}}}
  wantarray? @sources: \@sources;
}

sub preprocessors {
  my $self=shift;
  if (@_) {
    my %id2preprocessor;
    my @lines=split("\n",$_[0]);
    # each line is key => class ( params )
    #   => and params are optional
    for (@lines) {
#      my($key,$class,$skip,$skip,$params)=/^\s*(\w+)\W+(\w+(::\w+)*)(\s*\((.*?)\)\s*)*$/;
      my($key,$class,$skip,$params)=/^\s*(\w+)\W+(\w+(::\w+)*)\s*(.*)\s*$/;
      $params=~s/^\(|\)$//g;
      $id2preprocessor{quotemeta($key)}=[$class,$params];
    }
    $self->_id2preprocessor(\%id2preprocessor);
  }
}

sub resource_pool {
  my $self=shift;
  my $pool=@_ && $_[0]? $_[0]: $self->_resource_pool;
  if (@_) {
    $self->is_prepared(0); 
    $pool=$self->_resource_pool
      (ref $pool? $pool: 
       new Data::Workflow::ResourcePool(-id=>$pool,-create_flag=>$self->create_flag));
  }
  $pool;
}
sub namespaces {
  my $self=shift;
  my $pool=$self->resource_pool;
  if (@_) {
    $self->is_prepared(0); 
    my $namespaces=[parse_list($_[0])];
    $pool=$self->resource_pool($pool) unless ref $pool;	# can't wait any more to make the pool...
    $pool->parse_namespaces($namespaces);
  }
  my $namespaces=ref $pool? $pool->namespaces: [];
  wantarray? @$namespaces: $namespaces;
}
sub version {
  my $self=shift;
  my $version=@_? $self->_version(shift): $self->_version;
  my $from=@_? $self->_from(shift): $self->_from;
  # get VersionMap if necessary and possible
  if ($version && !ref $version) {
    my $pool=$self->resource_pool;
    $version=$self->_version($pool->get_vmap($version) || $pool->add_vmap($version,$from)) 
      if ref $pool;
  } 
  $version;
}
sub disease_id {
  my $self = shift;
  my $disease_id = @_? $self->_disease_id(shift): $self->_disease_id;
  $disease_id;
}
sub resources {
  my $self=shift @_;
  confess "Cannot set Workflow::resources.  Have to use add_resources" if @_;
  my @resources=values %{$self->id2resource};
  wantarray? @resources: \@resources;
}
sub id2resource {
  my $self=shift @_;
  my $pool=$self->resource_pool;
  confess "Cannot access id2resource unless Workflow::resource_pool is set" unless $pool;
  $pool->id2resource(@_);
}
sub add_resources {
  my $self=shift;
  my @resource_ids=flatten(@_);
  my $pool=$self->resource_pool;
  confess "Can't add resources unless Workflow::resource_pool is set" unless ref $pool;
  # use 'get' since it also adds resource. do it this way, because original version 
  # called 'get' after 'add'.  dunno if really needed
  $pool->get_resources(@resource_ids);
}
# created is SCALAR ref used as out param to indicate that new Resource created
sub add_resource {
  my($self,$resource_id,$created)=@_;
  my $pool=$self->resource_pool;
  confess "Can't add resources unless Workflow::resource_pool is set" unless ref $pool;
  my $resource=$self->id2resource->($resource_id);
  return $resource if $resource; # resource already exists
  $$created++;
  $pool->add_resource($resource_id);
}
# Add namespaces for resources that are uri's
sub add_namespaces {
  my($self,$pool)=@_;
  my @namespaces;
  for my $input (@{$self->inputs}) {
    if ($input=~/^\w+:/) {	     # it's a uri
      my($scheme)=$input=~/^(\w+:)/; # eg, 'ftp:'. id for generic uri's
      my $namespace=$pool->add_namespace(new Data::Workflow::Namespace::uri(-id=>$scheme));
      push(@namespaces,$namespace) if $namespace;
    } # else assume it's already a Resource name and Namespace included in normal specification
  }
  wantarray? @namespaces: \@namespaces;
}

# NG 11-01-05: added make_namespace. usually defined in GLOBAL value can be 
#   1) type, eg, 'uri' or 'GDxBase::Workflow::Namespace::IxnSource', or 
#   2) hash of parameter=>type, eg, homepage=>GDxBase::Workflow::Namespace::IxnSource
sub parse_make_namespace {
  my($make_namespace,%step_params)=@_;
  my $namespace;
  my %param2nstype=parse_hashl($make_namespace);
  my @values=values %param2nstype;
  return $values[0] if @values==1 && !length($values[0]); # form 1
  # form 2. match parameters
  my @matches=grep {exists $param2nstype{$_}} keys %step_params;
  return $param2nstype{$matches[0]} if @matches==1;	 # form 2. uniqe match
  confess "ambiguous make_namespace parameters: @matches" if @matches>1;
  return;			                         # false alarm. no match
}
sub make_namespace {
  my($self,$type,$id,%step_params)=@_;
  my $pool=$self->resource_pool;
  my $namespace=$pool->new_namespace($type,$id,%step_params);
  $pool->add_namespace($namespace);
  $namespace->put;		# put immediately since may be needed by later stages
}

sub get_namespace {
  my($self,$namespace_id)=@_;
  my $pool=$self->resource_pool;
  confess "Cannot get namespaces unless Workflow::resource_pool is set" unless ref $pool;
  $pool->get_namespace($namespace_id);
}
sub get_namespaces {
  my $self=shift @_;
  my $pool=$self->resource_pool;
  confess "Cannot get namespaces unless Workflow::resource_pool is set" unless ref $pool;
  $pool->get_namespaces(@_);
}
sub get_resource {
  my($self,$resource_id)=@_;
  my $pool=$self->resource_pool;
  confess "Cannot get resources unless Workflow::resource_pool is set" unless ref $pool;
  $pool->get_resource($resource_id);
}
sub get_resources {
  my $self=shift @_;
  my $pool=$self->resource_pool;
  confess "Cannot get resources unless Workflow::resource_pool is set" unless ref $pool;
  $pool->get_resources(@_);
}
# args are mode, resource, created (out param)
sub get_version {
  my $self=shift @_;
  my $vmap=$self->version;
  confess "Cannot get versions of resources unless Workflow::version is set" unless $vmap;
  $vmap->get_version(@_);
}
# args are mode, resources
sub get_versions {
  my $self=shift @_;
  my $vmap=$self->version;
  confess "Cannot get versions of resources unless Workflow::version is set" unless $vmap;
  $vmap->get_versions(@_);
}
# convenience method that get all READ versions
sub versions {
  my($self)=@_;
  my @versions=$self->get_versions(VERSION_READ,$self->resources);
  wantarray? @versions: \@versions;
}
sub prepare {
  my ($self)=@_;
  return if $self->is_prepared;

  my $graph=$self->graph;	# construct dependency graph
  # traverse the graph and let Steps transform their local connectivity.
  # currently needed by copy
  my $sort=new Data::Workflow::BFSort(-graph=>$graph);
  my $changed=grep {$_} map {$_->transform($self)} $sort->get_all;
  if ($changed) {		# remake graphs
    $self->graph(undef);	# undef triggers recompute
  }
  # connect pipeline to resources. 
  # do after transform to take into account changed resources
  # TODO: check if still needed
  $self->prep_resources;

  ########################################
  # NO LONGER USED
#  # let Steps validate selves: currently, check input/output types and numbers. 
#  # have to do last. CAUTION: will crash if done before resources.
#  my $pool=$self->resource_pool;
#  map {$_->validate($pool)} $self->steps; 
  ########################################

  $self->is_prepared(1);	# so won't run again
}
sub prep_resources {
  my $self=shift @_;
  my $pool=$self->resource_pool;
  $pool=$self->resource_pool($pool) unless ref $pool;	# can't wait any more to make the pool...
  $pool->log($self->log);
  my @namespaces=$self->namespaces; # in addition to needing value, namespaces should be called
                                    # here in case originally called before pool set
  my @inputs=uniq(map {$_->inputs} $self->steps);
  my @outputs=uniq(map {$_->outputs} $self->steps);
  my @resource_ids=uniq(@inputs,@outputs);
  my @resources=$pool->add_resources(@resource_ids);
#  my @resources=$pool->get_resources(@resource_ids);
  # NG 06-05-10: move code down after letting Steps add inputs and outputs to resources
  # NG 06-04-05: add Resources for Namespaces (ie, Resource whose id equals Namespace)
  #              people seem to expect these to exist
  # NG 06-05-10: don't do it for null namespaces ans this creates strange Resources 
  #              with empty names
  # TODO: right solution is to really support Resource hierarchies...
  my @ns_resources=$self->add_resources(map {$_->id} grep {$_->type ne 'null'} @namespaces);
}
sub execute {
  my($self,@args)= @_;
  @args=(-mode=>$args[0]) if @args==1 && !ref $args[0];
  my $args=new Hash::AutoHash::Args(@args);
  my($mode,$steps,$sources,$targets,$exclude)=@$args{qw(mode steps sources targets exclude)};

  $mode or $mode=EXECUTE_OOD;
  if ($mode=~/\p{IsAlpha}/) {	    # contains alpha. assume string
    $mode=~s/^\s+|\s+$//g;	    # string leading and trailing white space
    $mode=~s/[\s\p{IsPunct}]+/_/g;  # convert white space or punctuation to _
    my $long_mode=$LONG_EMODES{$mode};
    $mode=$EXECUTE_MODES{$long_mode};
  }
  confess "Invalid execute mode $mode: should be 'old', 'out_of_date', 'force', 'all', EXECUTE_OOD, or EXECUTE_FORCE, show, EXECUTE_SHOW or prefixes thereof"
    unless grep {$mode==$_} (EXECUTE_OOD,EXECUTE_FORCE,EXECUTE_SHOW);
  $steps=parse_list($steps);
  $sources=parse_list($sources);
  $targets=parse_list($targets);
  $exclude=parse_list($exclude);
  confess "Invalid execute parameters. Cannot specify -step and either or both of -sources or -targets" if @$steps && (@$sources || @$targets);

  # prepare object.  makes graph
  {
      local $SIG{__DIE__} = sub { confess @_ };
      $self->prepare;
  }

  # get graph of dependencies we care about
  my $graph=$self->graph;
  if (@$steps) {
    my @steps=$self->get_steps($steps);
    unless (scalar @steps==scalar @$steps) { # one or more invalid step ids
      my $id2step=$self->id2step;
      my @bad_ids=grep {!exists $id2step->{$_}} @$steps;
      confess "Invalid step ids in -steps: @bad_ids";
    }
    $graph=$self->subgraph($graph,@steps);
  } else {
    if (@$sources) {
      my @steps=$self->get_steps($sources);
      unless (scalar @steps==scalar @$sources) { # one or more invalid step ids
	my $id2step=$self->id2step;
	my @bad_ids=grep {!exists $id2step->{$_}} @$sources;
	confess "Invalid step ids in -sources: @bad_ids";
      }
      $graph=$self->descendant_subgraph($graph,@steps);
    }
    if (@$targets) {
      my @steps=$self->get_steps($targets);
      unless (scalar @steps==scalar @$targets) { # one or more invalid step ids
	my $id2step=$self->id2step;
	my @bad_ids=grep {!exists $id2step->{$_}} @$targets;
	confess "Invalid step ids in -target: @bad_ids";
      }
      $graph=$self->ancestor_subgraph($graph,@steps);
    }
  }
  my %exclude;			# Steps to exclude
  if (@$exclude) {
    my @steps=$self->get_steps($exclude);
    unless (scalar @steps==scalar @$exclude) { # one or more invalid step ids
      my $id2step=$self->id2step;
      my @bad_ids=grep {!exists $id2step->{$_}} @$steps;
      confess "Invalid step ids in -exclude: @bad_ids";
    }
    @exclude{@steps}=@steps;
  }
  # traverse the graph and execute!
  my $sort=new Data::Workflow::BFSort(-graph=>$graph);
  while (my @ready=$sort->get_next) {        # get_next returns all ready Steps
    @ready=grep {!$exclude{$_}} @ready;
    print join(', ',sort map {$_->id} @ready),"\n" if $mode==EXECUTE_SHOW;
    # group Steps by type, since some types prefer to process many Steps at once
    my %groups=group(sub {$_->type},@ready);
    while(my($type,$steps)=each %groups) {
      next unless @$steps;
      # NG 10-07-30: 'class' method no longer exists in Class::AutoClass
      # my $class=$steps->[0]->class;
      my $class=ref($steps->[0]);
      $class->execute_list($self,$mode,$steps) unless $mode==EXECUTE_SHOW;
    }
  }
#  $self->put;			# 'put' moved into Step::execute_list and subclasses
}
sub steps {
  my($self)=@_;
  confess "no self->id2step" unless $self->id2step;
  confess "self->id2step not a hash ref" unless ref $self->id2step eq 'HASH';
  my @steps=values %{$self->id2step};
  wantarray? @steps: \@steps;
}
sub get_steps {
  my $self=shift @_;
  my @step_ids=flatten(@_);
  my $id2step=$self->id2step;
  my @steps=grep {defined $_} @$id2step{@step_ids};
  wantarray? @steps: \@steps;
}
sub get_step {
  my($self,$step_id)=@_;
  $self->_id2step->{$step_id};
}
sub step_ids {
  my($self)=@_;
  my @step_ids=keys %{$self->id2step};
  wantarray? @step_ids: \@step_ids;
}

sub new_step {
  my($self,$step_class,$type,$step_id,$format,$inputs,$outputs,$main_param,%step_params)=@_;
  eval "use $step_class";
  if ($@) {			# 'use' failed
    confess "Unknown step type $type for class $step_class" if $@=~/^Can\'t locate $step_class/;
    confess $@;
  }
  # NG 06-05-31: when type is a ::'ed path, it doesn't work as a method name.
  #              use last word instead. would probably be better to use the
  #              name 'main_param' for the main_parameter instead of equating
  #              it to $type
  my $type_param=$type;
  $type_param=~s/^.*\W//;
#  my %step_params=%$step_params;
  unless ($type eq $type_param) {
    $step_params{$type_param}=$step_params{$type};
    delete $step_params{$type};
  }
  if ($step_params{$type_param} && $main_param) { # it is legal, but strange, for both to be set
    $step_params{$type_param}="$step_params{$type_param} $main_param";
  } elsif ($main_param) {
    $step_params{$type_param}=$main_param;
  }
  my $step=new $step_class (-step_id=>$step_id,-type=>$type,-step_format=>$format,-pipeline=>$self,
			    -inputs=>$inputs,-outputs=>$outputs,%step_params);
  $self->id2step->{$step_id}=$step;
  $step;
}

sub id2step {
  my $self=shift @_;
  return $self->_id2step unless @_;
  return $self->_id2step($_[0]) if 'HASH' eq ref $_[0];
  return $self->_id2step->{$_[0]};
}
# values are '[class params]'. params is a string, not surrounded by () 
sub id2preprocessor {
  my $self=shift @_;
  return $self->_id2preprocessor unless @_;
  return $self->_id2preprocessor($_[0]) if 'HASH' eq ref $_[0];
  return $self->_id2preprocessor->{$_[0]};
}

# these belong in some Util. $obj is anything that w/ get_namespace method
# prepend prefix to resource ids unless id is 'rooted', ie, begins with '/'
# CAUTION: I wanted to also exclude resources that started with namespace, eg, Import/xyz,
#          but too many resources share the same name as the URL namespace from which
#          they are derived
sub prepend {
  my($prefix,@resource_ids)=@_;
  if ($prefix) {
    $prefix=~s/\/$//;
    @resource_ids=flatten(@resource_ids);
    @resource_ids=map {prepend1($prefix,$_)} @resource_ids;
  }
  return wantarray? @resource_ids : 
    @resource_ids==1 & !ref $resource_ids[0]? $resource_ids[0]: \@resource_ids;
}
# my $ns_pattern='^(.*?)(\/|$)';	# set pattern in a variable to keep emacs happy...
sub prepend1 {
  my($prefix,$resource_id)=@_;
  return $resource_id if $resource_id=~/^\//;
  return $resource_id ne '.'? $prefix.'/'.$resource_id: $prefix;
}
# crock to preserve compatibilty with old prefix behavior
# TODO: elimimate need for this case by fixing conf files
my $ns_pattern='^(.*?)(\/|$)';	# set pattern in a variable to keep emacs happy...
sub prepend_compat {
  my($obj,$prefix,@resource_ids)=@_;
  my %group=ns_group($obj,@resource_ids);
  my @out=@{$group{skip}};
  push @out,prepend($prefix,@{$group{use}});
  @out;
}
 
# TODO: note 'group' defined w/o prototype near bottom of file
sub ns_group {
  my($obj,@r_ids)=@_;
  my %group=(skip=>[],use=>[]);
  for my $r_id (@r_ids) {
    my($ns_id)=$r_id=~/$ns_pattern/;
    my $ns= $obj->get_namespace($ns_id);
    if ($ns) {
      my $ns_type=$ns->type;
      push(@{$group{skip}},$r_id), next if $ns_type eq 'path' || $ns_type eq 'database';
    }
    push(@{$group{use}},$r_id);
  }
  wantarray? %group: \%group;
}

# take into account hierarchical resources
# if step S1 has output A/B/C, S1 triggers any step with input is A or  A/B or A/B/C or A/B/C/D ...
# Design:
# 1) resources R1 and R2 "match" if their ids are equal or one is a prefix of the other
# 2) for each output, find matching inputs
# 3) for each step, for each output, for each matching input
# CAUTION:
# this simple algortihm is O(n^2) so may become a performance problem
# if so, consider binary search, trie, or suffix tree
sub graph {
  my $self=shift @_;
  my $graph=@_? $self->_graph($_[0]): $self->_graph;
  return $graph if $self->is_prepared && defined $graph;
  # else make graph. nodes are Steps; edges depict dependencies
  my(%resource2producers,%resource2consumers,%output2inputs);
  my $graph=new Graph::Directed;
  my @steps=$self->steps;
  map {$graph->add_vertex($_)} @steps; # NG 08-07-14: line needed to handle isolated nodes
  for my $step (@steps) {
    map {push(@{$resource2consumers{$_}},$step)} $step->inputs;
    map {push(@{$resource2producers{$_}},$step)} $step->outputs;
  }
  my @inputs=keys %resource2consumers;
  my @outputs=keys %resource2producers;
  for my $output (@outputs) {
    for my $input (@inputs) {
      push(@{$output2inputs{$output}},$input) 
	if $output eq $input || $output=~/^$input\// || $input=~/^$output\//;
    }}
  uniq_values(\%resource2consumers);
  uniq_values(\%resource2producers);
  uniq_values(\%output2inputs);
  my %seen;
  while(my($output,$producers)=each %resource2producers) {
    for my $input (@{$output2inputs{$output}}) {
      for my $producer (@$producers) {
	for my $consumer (@{$resource2consumers{$input}}) {
	  $graph->add_edge($producer,$consumer) unless $seen{"$producer$;$consumer"}++;
	}}}}
  confess "Dependency graph is cyclic.  Sorry I can't point you to the problem,.." 
    if $graph->is_cyclic;
  $self->_graph($graph);

#   # for testing -- print graph in legible format
#   my @edges=$graph->edges;
#   my @edge_strings=map {$_->[0]->id.' -> '.$_->[1]->id."\n"} @edges;
#   @edge_strings=sort @edge_strings;
#   print @edge_strings;
#   # and test for cycles
#   print "\$graph is_cyclic? ",($graph->is_cyclic? 'yes': 'no'),"\n";
#   $graph;
}
sub uniq_values {
  my($hash)=@_;
  return unless $hash;
  map {@$_=uniq(@$_)} values %$hash;
  $hash;
}

sub log {
  my $self=shift;
  return $self->_log unless @_;
  my $log=
    blessed($_[0]) && $_[0]->isa('Data::Workflow::Log')? $_[0]: # use log if supplied
      new Data::Workflow::Log(@_);	                 # else, make from params
  $self->log($log);
  my $pool=$self->resource_pool;
  $pool->log($log) if ref $pool; # store in pool if it exists
  $log;
}

# return true if a parameter (really any string) is a pre-defined pipeline parameter
sub is_pipe_param {
  my($param)=@_;
  grep(/$param/,@PIPE_PARAMS) || $param=~/^pipeline_/;
}
# return true if a parameter (really any string) is a pre-defined section parameter
# NG 11-01-06: fixed dumb bug in grep. was returing true if $param matched ANY SUBSTRING of
#              any section param!  scary this wasn't caught before....
sub is_sect_param {
  my($param)=@_;
  # grep /$param/,@SECT_PARAMS;
  first {$_ eq $param} @SECT_PARAMS;
}
# return true if a parameter (really any string) is a step parameter
#   anything that's not a pipeline or section parameter
sub is_step_param {
  my($param)=@_;
  !(is_pipe_param($param) || is_sect_param($param));
#  $type? $param=~/^${type}_/ || $param eq $type: undef;
}
# test validity of format value and convert to short form
sub short_format {
  my($format)=@_;
  return unless $format;
  my $long_format=$FORMATS{$format};
  confess "Unrecognized format $format" unless $long_format;
  $SHORT_FORMATS{$long_format};
}
# convert possibly empty text lists into Perl lists. do not eliminate duplicates
sub param_list {
  my @list=split(/\s+/,$_[0]);
  wantarray? @list: \@list;
}
# convert member list into Perl structure. two cases
# 1) list of words. just split and return as ARRAY
# 2) list of param=value lines. return as HASH
sub parse_members {
  my($members)=@_;
  unless ($members=~/=/) {
    my @list=split(/\s+/,$members);
    return wantarray? @list: \@list;
  } else {
#    my %hash=map /^\s*(\S+)\s*=\s*(.*\S)\s*$/gm,$members;
    my %hash;
    my @lines=split(/\n/,$members);
    while(@lines) {
      $_=shift @lines;
      my($param,$value)=/^\s*(\S+?)\s*=\s*(.*\S)\s*$/;
      if (my($eos)=$value=~/^<<(\S+)/) { # multi-line value
	my @value;
	while(@lines) {
	  $_=shift @lines;
	  last if /^$eos$/;
	  push(@value,$_);
	}
	$value=join("\n",@value);
      }
      $hash{$param}=$value;
    }
    return wantarray? %hash: \%hash;
  }
}

# group resource list (eg, inputs, outputs) by member
sub member2resources {
  my($resources,$members)=@_;
  my($member2resources,$general_resources)=({},[]);
  for my $resource (@$resources) {
    my @name=splitname($resource);
    my $member=$name[$#name];         # get last component
    my $list;
    if (exists $members->{$member}) { # it's a member resource
      $list=$member2resources->{$member} || ($member2resources->{$member}=[]);
    } else {			      # it's a general resource
      $list=$general_resources;
    }
    push(@$list,$resource);
  }
  wantarray? ($member2resources,$general_resources): $member2resources;
}

# NG 06-05-12:# code below not tested, but written to implement the behavior labelled 
#               'not yet implemented' in the docs
#sub autodb {
#  my $self=shift;
#  if (@_) {
#    my $autodb=$_[0];
#    unless (UNIVERSAL::isa($autodb,'Class::AutoDB')) {         # not given AutoDB. what is it?
#      if (ref $autodb) {
#	confess "Invalid parameter to autodb: refs should be HASHes or ARRAYs, not ".ref $autodb
#	  unless 'HASH' eq ref $autodb || 'ARRAY' eq ref $autodb;
#	$autodb=new Class::AutoDB($autodb);                  # HASH or ARRAY: make AutoDB
#      } elsif (defined $autodb || !$autodb) {                # scalar FALSE value...
#	$autodb=undef;		                             # ...signals non-peristent operation
#      } else {                                               # default case...
#	$autodb=Class::AutoDB::Globals->instance()->autodb;  # ...use global AutoDB
#      }}
#    $self->_autodb($autodb);
#  }
#  $self->_autodb;
#}
# NG 06-05-12: code below duplicated in ResourcePool
# TODO: refactor to eliminate duplication!
sub autodb {
  my $self=shift;
  my $autodb=Class::AutoDB::Globals->instance()->autodb;
  if (@_) {
    if (ref $_[0]) {
      confess "Invalid parameter to autodb: refs should be HASHes or ARRAYs, not ".ref $autodb
	unless 'HASH' eq ref $_[0] || 'ARRAY' eq ref $_[0];
      if ($autodb) {
	$autodb->renew(@_);
      } else {
	$autodb=new Class::AutoDB(@_);
      }
    } elsif ($autodb && defined $_[0] && !$_[0]) {	# defined but FALSE
      $autodb->disconnect;
    }
  }
  if (!$autodb->dbh->ping()) {
     $autodb->renew;
  }
  $autodb or $autodb=new Class::AutoDB; # this 'new' sets global autodb
}

# this is a very blunt way of putting the object
sub put {
  my($self)=@_;
  return unless $self->autodb->is_connected;
  map {$_->put} ($self->steps,$self->resource_pool);
  $self->SUPER::put;
}
# just put the Workflow object -- no dependents
sub put_object {$_[0]->SUPER::put}

# convert classname to path & vice versa: just switch :: and /
# make sure input is copied so replacement is not in-place (original unchanged)
sub class2path {my $class=$_[0]; $class=~s/::/\//g; $class}
sub path2class {my $path=$_[0]; $path=~s/\//::/g; $path}

# interpolate literals (eg, __SECTION__) in hash values
sub interp_literals {
  my($hash,$literals)=@_;
  my $pattern=join('|',keys %$literals);
  map {s/($pattern)/$literals->{$1}/g} values %$hash;
  $hash;
}
1;


__END__

=head1 NAME

Data::Workflow - Workflow manager for loading biological databases 

=head1 VERSION

Version 0.10

=head1 SYNOPSIS

  use Data::Workflow;

=head1 DESCRIPTION

This version is just a placeholder.  It does not work. A real version
will appear soon.

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Institute for Systems Biology

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1; # End of Data::Workflow
