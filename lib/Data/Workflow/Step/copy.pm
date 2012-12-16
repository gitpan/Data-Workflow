package Data::Workflow::Step::copy;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-11-19
# $Id: 
#
# Step subclass for step type 'copy' -- handle ftp, http, and local copying
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
use File::Path;
use Net::FTP;
use LWP::UserAgent;
use HTTP::Request;
use HTTP::Date;
use HTTP::Status;
use Text::Glob qw(glob_to_regex);
use Data::Workflow;
use Data::Workflow::Step;
use Data::Workflow::Step::copy::Operation;
use Data::Workflow::Util qw(choose_basepath choose_file clean_path file flatten group max uniq);
use Data::Workflow::VersionMap;
use Data::Workflow::Namespace::uri;
use Data::Dumper;

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES @CLASS_ATTRIBUTES %SYNONYMS %DEFAULTS);
@ISA = qw(Data::Workflow::Step);

@AUTO_ATTRIBUTES=qw(copy ftp_user ftp_pass 
		    http_realm http_user http_pass http_params
		    db_query
		    files _resources _input2outputs _input2patterns);
@OTHER_ATTRIBUTES=qw(input2outputs input2patterns);
@CLASS_ATTRIBUTES=qw(verbose);
%SYNONYMS=();
%DEFAULTS=(input2outputs=>{},input2patterns=>{},ftp_user=>'anonymous',ftp_pass=>'-anonymous@',
	   verbose=>0);
Class::AutoClass::declare(__PACKAGE__);

sub _init_self {
  my ($self, $class, $args) = @_;
  next unless $class eq __PACKAGE__; # so subclasses won't run this
  my $id=$self->id;
  my $pipeline=$args->pipeline;
  # map files (which may be from inputs, 'main' param, or files) to outputs
  # note: inputs, files, 'main' considered equivalent
  # cases
  # 1) 1 or more inputs, 1 output
  #    output is directory. inputs will be copied to output directory
  # 2) 1 input, multiple outputs
  #    input will be broadcast to all outputs
  # 3) multiple inputs, multiple outputs
  #    must have same number of inputs. inputs matched to outputs in order.
  my @inputs=$self->inputs;
  my @outputs=$self->outputs;
  my($param,$files)=($self->copy,$self->files);	# don't use get. confused by context.
  my($input2out,$renames)=({},{});
  $self->parse_files(\@inputs,$pipeline,$input2out,$renames) if @inputs;
  push(@inputs,$self->parse_files($param,$pipeline,$input2out,$renames)) if $param;
  push(@inputs,$self->parse_files($files,$pipeline,$input2out,$renames)) if $files;
  confess "Invalid parameters to step $id: no inputs, files, or 'main' parameter" unless @inputs;

  if (@outputs==1) {		      # case 1
    my $output=$outputs[0];
    for my $input (@inputs) {
      my $out=$input2out->{$input};
      $self->input2outputs($input,clean_path("$output/$out"));
    }
  } elsif (@inputs==1) {	      # case 2
    my $input=$inputs[0];
    my $out=$input2out->{$input};
    map {$self->input2outputs($input,clean_path("$_/$out"))} @outputs;
  } elsif (@inputs>1 && @outputs>1) { # case 3
    confess "Invalid inputs/outputs to step $id: when there are multiple inputs and outputs, the numbers have to match"
      unless (@inputs-keys %$renames)==@outputs; # ignore inputs matched via in=>out syntax
    for my $input (@inputs) {
      if ($renames->{$input}) { # already matched
	my $out=$input2out->{$input};
	$self->input2outputs($input,clean_path("$id/$out"));
      } else {			# match to output
	my $output=shift @outputs;
	$self->input2outputs($input,$output);
      }
    }
    push(@outputs,$id) if keys %$renames; # need to include step id, since files stored there
  }
  $self->inputs(uniq(@inputs)); # includes guys added from files and param
  push(@outputs,values %{$self->input2outputs});
  $self->outputs(uniq(@outputs));
}

# override Step method
sub execute_list {
  my($class,$pipeline,$mode,@steps)=@_;
  @steps=flatten(@steps);
  my $log=$class->log($pipeline->log);		# store in class for easy use by subclasses
#  print $log "BEGIN execute_list: ",join(' ',map {$_->id} @steps);

  # create Operation objects from Steps
  my @ops;
  for my $step (@steps) {
    my @inputs=$step->inputs;
    for my $input (@inputs) {
      my $outputs=$step->input2outputs($input);
      my $patterns=$step->input2patterns($input);
      push(@ops,
	   ops Data::Workflow::Step::copy::Operation($pipeline,$step,$input,$outputs,$patterns));
    }}
  # group and exectue by scheme
  # note: local must be last for $extra_copies to work
  my %by_scheme=group {$_->scheme} @ops;
  my $extra_copies=[];
  $class->execute_ftp($pipeline,$mode,$by_scheme{ftp},$extra_copies);
  $class->execute_http($pipeline,$mode,$by_scheme{http},$extra_copies);
  $class->execute_database($pipeline, $mode, $by_scheme{database}, $extra_copies);
  push(@{$by_scheme{local}},@$extra_copies);
  $class->execute_local($pipeline,$mode,$by_scheme{local});
#  print $log "END execute_list";
}
sub execute_ftp {
  my($class,$pipeline,$mode,$ops,$extra_copies)=@_;
  my $log=$pipeline->log;  
  # group by site, user, password
  my %by_site=group {join($;,$_->site,$_->ftp_user,$_->ftp_pass)} @$ops;
  while(my($group,$ops)=each %by_site) {
    my($site,$user,$pass)=split($;,$group);
    my $site_status = 1;
    print $log "FTP connect: site=$site, user=$user, pass=$pass\n";
    my $ftp=Net::FTP->new("$site", Debug => 0, Passive => 1) or $site_status = 0;
    if ($site_status == 0) {
       warn "UNKNOWN HOST $site";
       next;
    }
    eval {
      $ftp->login($user,$pass);
      $ftp->binary();
    };
    if ($@) {
      $log->error("FTP connect failure: $@");
      # TODO: set state for failed inputs and outputs
      last;
    }
    print $log "FTP connect ok";
    # group by input to process patterns.
    # pattern means user wants multiple files from input directory.
    # this loop creates a 'regular' op (one without a pattern) for each file.
    # later, re-group the regular ops and get the file
    # NG 08-09-19: trashed $pool_changed - updates now handled by VersionMap and ResourcePool
    my %by_input=group {$_->real_input} @$ops;  
    my @regular_ops;
    while(my($real_input,$ops)=each %by_input) {
      if (has_pattern($ops)) {	# also checks for consistency
	# NG 08-09-19: changed call to 'in_filespare' to 'in_path' since other components not used
	# my($in_dir,$in_file,$in_path)=$ops->[0]->in_fileparse;
	my $in_path=$ops->[0]->in_path;
#	print $log "BEGIN ls $in_path";
	my @files=map {file($_)} $ftp->ls("$in_path");
#	print $log "END ls: got ".scalar @files." files";
	push(@regular_ops,regular_ops($ops,$pipeline,@files));
      } else {
	push(@regular_ops,@$ops);
      }}
    # regroup by input to do real work!
    my %by_input=group {$_->real_input} @regular_ops;
    while(my($real_input,$ops)=each %by_input) {
      # get one file.  input is file
      my $site_state = 1;
      my $max_modtime=max(map {$_->output->modtime} @$ops);
      my $outputs_exist=$max_modtime? 1: 0;
      my $op=$ops->[0];
      # NG 08-09-19: moved code that deals with output after 'upgrade_output'
      #              part of fix for bug that created new version even if old version exists
      #              also changed call to 'in_filespare' to 'in_path' since other components
      #                not used
      # my($input,$output)=($op->input,$op->output);
      # my($in_dir,$in_file,$in_path)=$op->in_fileparse;
      # my($out_dir,$out_file,$out_path)=$op->out_fileparse;
      my $input=$op->input;
      my $in_path=$op->in_path;
      print $log "CHECK modtime for $in_path";
      my $remote_modtime=$ftp->mdtm($in_path);
      print $log "modtime=",scalar localtime($remote_modtime);
      $input->checktime(time);
      
      my $download_ok;
      # pay attention to remote_modtime unless
      # 1) forced execution
      # 2) none of outputs exist ($output->modtime == 0)
      # 3) never retrieved the input ($input->remote_modtime == 0)
      if ($mode==EXECUTE_FORCE || !$outputs_exist || $remote_modtime>$input->remote_modtime) {
	$input->remote_modtime($remote_modtime) if $remote_modtime>$input->remote_modtime; 
	$input->modtime(time);
	# NG 08-09-17: upgrade output version from VERSION_LATEST to VERSION_ WRITE
	#              fixes bug that created new version even if old version exists
	$op->upgrade_output($pipeline);
	my $out_path=$op->out_path;
	$class->out_dir($op,$ops);    # check and possibly create output directory
	print $log "BEGIN get $in_path to $out_path";
	$ftp->get($in_path,$out_path) or $site_state = -1;
        if ($site_state < 0) {
          warn "Cannot get file $real_input: ".$ftp->message;
	  print $log "FAILED get $real_input: ".$ftp->message."\n";
          next;
        }
	$download_ok=1;
	print $log "END get";
      }
      update_times($ops,$download_ok); # update modtime, checktime, etc. on other inputs
      push(@$extra_copies,extra_copies($ops,$download_ok)); # deal with extra copies, if any
    }}
}
sub execute_http {
  my($class,$pipeline,$mode,$ops,$extra_copies)=@_;
  my $log=$pipeline->log;
  my %by_input=group {$_->real_input} @$ops;

  # NG 08-10-07: changes by Denise to allow Curl-like patterns
  my @regular_ops;
  while(my($real_input,$ops)=each %by_input) {
    # confess "Copy does not support patterns on http inputs.  Check".
    #   join('; ',map {'step='.$_->step_id.', input='.$_->input_id} @$ops)
    #     if has_pattern($ops); # also checks for consistency
    if (has_pattern($ops)) {  # http_ops splits on , in the pattern
      push(@regular_ops, http_ops($ops, $pipeline));
    } else {
      push(@regular_ops, @$ops);
    }
  }
  # regroup after expanding patterns
  my %by_input = group {$_->real_input} @regular_ops;
  while (my($real_input,$ops)=each %by_input) {
    my $outputs_exist=grep {$_->output->modtime} @$ops;
    my $op=$ops->[0];
    my $input=$op->input;
    # NG 08-09-19: moved code that deals with output after 'upgrade_output'
    #              part of fix for bug that created new version even if old version exists
    # my($out_dir,$out_file,$out_path)=$op->out_fileparse;
    $input->checktime(time);

    my $http;
    my $http_params = $op->http_params;
    unless ($http_params) {	# regular 'get' uri
      $http=new HTTP::Request(GET=>$real_input);
    } else {			# http_params is set, so it's a 'post'
       $http = HTTP::Request->new("POST",$real_input,HTTP::Headers->new(),$http_params);
	print $log "HTTP REQUEST POST - $real_input?$http_params\n";
      #$http=new HTTP::Request(POST=>$real_input);
      #$http->content($http_params);
    }
    my $download_ok;
    # specify modtime-limit in get unless
    # 1) in force mode
    # 2) never retrieved the input ($input->remote_modtime == 0)
    # 3) none of outputs exist ($output->modtime == 0)
    my $remote_modtime=$input->remote_modtime;
    my $check_modtime;
    unless ($mode==EXECUTE_FORCE || !$remote_modtime || !$outputs_exist) {
      $http->header('If-Modified-Since',time2str($remote_modtime));
      $check_modtime=1;
    }
    my $agent=new LWP::UserAgent(cookie_jar=>{});
    # NG 08-07-11: set credential for server-level authentication
    $agent->credentials($input->host.':'.$input->port,
			$op->http_realm,$op->http_user,$op->http_pass) if $op->http_realm;
    # NG 08-09-17: upgrade output version from VERSION_LATEST to VERSION_ WRITE
    #              fixes bug that created new version even if old version exists
    $op->upgrade_output($pipeline);
    my $out_path=$op->out_path;
    $class->out_dir($op,$ops); # check and possibly create output directory
    print $log "BEGIN get $real_input to $out_path",($check_modtime? " with modtime limit": undef);
    my $result=$agent->request($http,$out_path);
    if ($result->is_success) {
      print $log "END get";
      my $remote_modtime=str2time $result->header('last_modified');
      $input->remote_modtime($remote_modtime);
      $input->modtime(time);
      $download_ok=1;
    } elsif ($result->code==RC_NOT_MODIFIED) {
      print $log "END get (content not modified)";
    } else {			# some error: common cases
				# 500 RC_INTERNAL_SERVER_ERROR -- usually means bad hostname
				# 404 RC_NOT_FOUND -- file not found
      warn "Cannot get file $real_input: ".$result->status_line;
      print $log "FAILED get $real_input: ".$result->status_line."\n";
      next;
    }
    update_times($ops,$download_ok); # update modtime, checktime, etc. on other inputs
    push(@$extra_copies,extra_copies($ops,$download_ok)); # deal with extra copies, if any
  }
}

sub execute_database {
  my($class,$pipeline,$mode,$ops,$extra_copies)=@_;
  my $log=$pipeline->log;
  my %by_input=group {$_->real_input} @$ops;
  while(my($real_input,$ops)=each %by_input) {
    confess "Copy does not support patterns on database inputs.  Check".
      join('; ',map {'step='.$_->step_id.', input='.$_->input_id} @$ops)
	if has_pattern($ops); # also checks for consistency
    # my $outputs_exist=grep {$_->output->modtime} @$ops;
    my $dbh = $ops->[0]->connect;
  
    # print $log "BEGIN getting information from database ".$ops->[0]->input->database;
    foreach my $op (@$ops) {
      my $input=$op->input;
      $input->checktime(time);
      $input->put;
      my $query = $op->step->db_query;
      if ($query =~ /delete/i) { 
        print $log "Query for step ".$op->step->id." tried to delete.  This is not allowed\n";
        next;
      } elsif ($query =~ /insert/i) {
        print $log "Query for step ".$op->step->id." tried to insert.  This is not allowed\n";
        next;
      }
      # NG 08-09-30: upgrade output version from VERSION_LATEST to VERSION_ WRITE
      #              needed for compatibility with version code in execute_ftp, _http
      $op->upgrade_output($pipeline);
      my $out_path=$op->out_path;
      $class->out_dir($op,$ops); # check and possibly create output directory
      my $OUTPUT = $op->out_open;
      print $log "BEGIN SELECT from database ".$op->database." for step ".$op->step_id." to $out_path";

      my $sth = $dbh->prepare($query) || die $dbh->errstr."\n";
      $sth->execute || die $dbh->errstr."\n";
      while (my @results = $sth->fetchrow_array) {
        print $OUTPUT join("\t", @results)."\n";
      }
      close($OUTPUT);
      print $log "END SELECT for step ".$op->step_id;
    }  # end of foreach
    
#    print $log "END retrieving database info from ".$ops->[0]->input->database;
  }
}

sub execute_local {
  my($class,$pipeline,$mode,$ops)=@_;
  my $log=$pipeline->log;
  # group by input to process patterns.
  # pattern means user wants multiple files from input directory.
  # this loop creates a 'regular' op (one without a pattern) for each file.
  # later, re-group the regular ops and get the file
  # NG 08-09-19: trashed $pool_changed - updates now handled by VersionMap and ResourcePool
  my %by_input=group {$_->real_input} @$ops;
  my @regular_ops;
  while(my($real_input,$ops)=each %by_input) {
    if (has_pattern($ops)) {	# also checks for consistency
      # NG 08-09-19: changed call to 'in_filespare' to 'in_path' since other components not used
      # my($in_dir,$in_file,$in_path)=$ops->[0]->in_fileparse;
      my $in_path=$ops->[0]->in_path;
#      print $log "BEGIN ls $in_path";
      my $dh;
      opendir($dh,$in_path) || confess "Cannot read directory $in_path: $!"; # TODO: need to close this somewhere?
      my @files=map {file($_)} grep !/^\.\.?$/,readdir($dh);
#      print $log "END ls: got ".scalar @files." files";
      push(@regular_ops,regular_ops($ops,$pipeline,@files));
    } else {
      push(@regular_ops,@$ops);
    }}
  # regroup by input to do real work!
  my %by_input=group {$_->real_input} @regular_ops;
  while(my($real_input,$ops)=each %by_input) {
    # copy one file.  input is file
    my $op=$ops->[0];
    my $input=$op->input;
    my($in_path,$in_suffix,$stat)=choose_file($op->in_path);
    my $modtime=$input->modtime;
    warn "Cannot get file $real_input: file does not exist" unless $modtime;
    print $log "FAILED get $real_input: file does not exist\n" unless $modtime;
    next unless $modtime;

    $input->checktime(time);
    $input->put;		# so checktime updated in database
    my %seen;			# to detect duplicate outputs
    for my $op (@$ops) {
      my($real_output,$output)=($op->real_output,$op->output);
      # don't copy if 
      # (1) duplicate output, (2) input same as output, or 
      # (3) mode isn't FORCE and input older than output
      next if $seen{$real_output}++ || $real_input eq $real_output || 
	($mode!=EXECUTE_FORCE && $modtime<=$output->modtime);
      # NG 08-09-17: upgrade output version from VERSION_LATEST to VERSION_ WRITE
      #              fixes bug that created new version even if old version exists
      $op->upgrade_output($pipeline);
      my $out_path=$op->out_path;
      $class->out_dir($op,$ops);    # check and possibly create output directory
      # NG 08-09-24: output must have same suffix as input
#      my($out_dir,$out_base,$out_suffix)=fileparse3($out_path);
      my $out_basepath=choose_basepath($out_path);
      $out_path=$out_basepath . ($in_suffix? ".$in_suffix": undef);
      print $log "BEGIN cp $in_path to $out_path";
      eval {system "cp $in_path $out_path"};
      confess $@ if $@;	# TODO: capture error correctly
#      $output->put;
      print $log "END cp";
    }}
}

# generate regular ops for a list of ops w/ patterns
# NG 08-09-19: trashed $pool_changed - updates now handled by VersionMap and ResourcePool
sub regular_ops {
  my($ops,$pipeline,@all_files)=@_;
  my @regular_ops;
  for my $op (@$ops) {
    my($step,$input,$output,$ls_pattern,$perl_pattern)=
      $op->get(qw(step input output ls_pattern perl_pattern));
    my @patterns;
    push(@patterns,glob_to_regex($ls_pattern)) if length $ls_pattern;
    push(@patterns,'('.$perl_pattern.')') if length $perl_pattern;
    my $pattern=join('|',@patterns);
    my @files=grep /$pattern/,@all_files;
    for my $file (@files) {
      # NG 08-09-19: changed loop to use 'get_resource' instead of 'add_resource' for clarity
      #              'get' methods below will create objects as needed
      my $input_id=$input->resource_id.'/'.$file;
      my $output_id=$output->resource_id.'/'.$file;
      my $input_resource=$pipeline->get_resource($input_id);
      my $output_resource=$pipeline->get_resource($output_id);
      my $input_version=$pipeline->get_version(VERSION_READ,$input_resource);
      my $output_version=$pipeline->get_version(VERSION_LATEST,$output_resource);
      push(@regular_ops,
	   new Data::Workflow::Step::copy::Operation
	   (-step=>$step,-input=>$input_version,-output=>$output_version));
    }}
  wantarray? @regular_ops: \@regular_ops;
}

# NG 08-10-07: changes by Denise to allow Curl-like patterns
# generate regular ops for a list of ops w/ patterns
# NG 08-10-07: trashed $pool_changed - updates now handled by VersionMap and ResourcePool
sub http_ops {
  my ($ops, $pipeline) = @_;
  my @regular_ops;
  for my $op (@$ops) {
    my($step,$input,$output,$ls_pattern,$perl_pattern)=
       $op->get(qw(step input output ls_pattern perl_pattern));
    my @patterns;
    push(@patterns,split(/, /,$ls_pattern)) if length $ls_pattern;
    map { $_ =~ s/@/[/; } @patterns;
    map { $_ =~ s/%/]/; } @patterns;
    my @new_steps;
    foreach my $pattern (@patterns) {
      if ($pattern =~ /\[/ && $pattern =~ /\]/) {
         $pattern =~ /(.*)\[(.*)-(.*)\](.*)/;
         for my $value ($2..$3) {
          push(@new_steps, $1.$value.$4);
         }
      }
    }
    for my $nstep (@new_steps) {
      # NG 08-10-07: changed loop to use 'get_resource' instead of 'add_resource' for clarity
      #              'get' methods below will create objects as needed
      my $input_id=$input->resource_id.'/'.$nstep;
      my $output_id=$output->resource_id.'/'.$nstep;
      my $input_resource=$pipeline->get_resource($input_id);
      my $output_resource=$pipeline->get_resource($output_id);
      my $input_version=$pipeline->get_version(VERSION_READ, $input_resource);
      my $output_version=$pipeline->get_version(VERSION_LATEST, $output_resource);
      push(@regular_ops, 
           new Data::Workflow::Step::copy::Operation 
	   (-step=>$step,-input=>$input_version,-output=>$output_version));
    }
  }
  wantarray? @regular_ops: \@regular_ops;
}

# update time attributes on inputs in list of ops
sub update_times {
  my($ops,$download_ok)=@_;
  my $input=$ops->[0]->input;
  my($remote_modtime,$modtime,$checktime)=$input->get(qw(remote_modtime modtime checktime));
  my @inputs=uniq(map {$_->input} @$ops[1..@$ops-1]);	# all but first op
  for my $input (@inputs) {
    $input->checktime($checktime); # always update checktime
    next unless $download_ok;	   # update others only if download happened
    $input->remote_modtime($remote_modtime);
    $input->checktime($checktime);
  }
  map {$_->put} ($input,@inputs);
}
# create copy operations for extra outputs
sub extra_copies {
  my($ops,$download_ok)=@_;
  my @extra_copies;
  if ($download_ok) {
    my $op=$ops->[0];
    @extra_copies=map 
      {new Data::Workflow::Step::copy::Operation
	 (-step=>$_->step,-input=>$op->output,-output=>$_->output)
       } @$ops[1..@$ops-1];	# all but first op
  } else {
    # input not copied (typically because it hasn't changed)
    # copy newest output to any others that do not exist
    my $max_modtime=max(map {$_->output->modtime} @$ops);
    my($op)=grep {$_->output->modtime==$max_modtime} @$ops;
    for (@$ops) {
      next if $_==$op || -e $_->real_output;
      push(@extra_copies,
	   new Data::Workflow::Step::copy::Operation
	   (-step=>$_->step,-input=>$op->output,-output=>$_->output));
    }
  }
  wantarray? @extra_copies: \@extra_copies;
}
# override Step method. not yet done.  probably gets trashed
# nyi
sub execute {
  my($self,$pipeline,$mode)=@_;
  print "Executing step: ",$self->step_id,"\n";
}
#sub resources {
#  my($self)=@_;
#  my @resources=_uniq($self->files,$self->outputs,flatten(values %{$self->input2outputs}));
#  wantarray? @resources: \@resources;
#}
#sub files {
#  my $self=shift @_;
#  my $files=@_? $self->_files([flatten(@_)]): $self->_files;
#  wantarray? 'ARRAY' eq ref $files? @$files: (): $files;
#}
sub input2outputs {
  my $self=shift;
  return $self->_input2outputs unless @_;
  return $self->_input2outputs($_[0]) if 'HASH' eq ref $_[0];
  my $hash=$self->_input2outputs;
  my $key=shift;
  my $list=$hash->{$key} || ($hash->{$key}=[]);
#  push(@$list,flatten(@_)) if @_;
  @$list=uniq(@$list,flatten(@_)) if @_;
  wantarray? @$list: $list;
}  
sub input2patterns {
  my $self=shift;
  return $self->_input2patterns unless @_;
  return $self->_input2patterns($_[0]) if 'HASH' eq ref $_[0];
  my $hash=$self->_input2patterns;
  my $key=shift;
  my $list=$hash->{$key} || ($hash->{$key}=[]);
  push(@$list,[flatten(@_)]) if @_;
  wantarray? @$list: $list;
}  
sub parse_files {
  my($self,$files,$pipeline,$input2out,$renames)=@_;
  my @inputs;
  my @lines='ARRAY' eq ref $files? @$files # already split
    : split(/\n/,$files);
  # specials
  # =>  rename
  # =*  ls pattern (pattern will be fed to 'ls' and matching files retrieved
  # =~  perl pattern (pattern will be evaluated in perl and matching files retrieved
  for (@lines) {
    s/^\s*|\s*$//g;
    # http 'get' synax should always include a => to rename the file
    if (/=/) {			# special syntax present
      my($in,%specials)=split(/\s*(=[\*>~])\s*/);  # 2008/03/03 DM
      $in=clean_path($in);
      # deal with empty patterns and various errors
      # be careful: hash keys (with undef values) spring into existence when touched!
      my $errstr="Error in step ".$self->id.", line $_";
      for my $special (keys %specials) {
	confess "$errstr. Unrecognized 'special' operator $special: should be =>, =>*, or =~"
	  unless grep /$special/,qw(=> =* =~);
	$specials{$special}=' ' if $special eq '=>' && !length($specials{$special});
	$specials{$special}='*' if $special eq '=*' && !length($specials{$special});
	$specials{$special}='.*' if $special eq '=~' && !length($specials{$special});
	next if $special eq '=>'; # rest of loop deals with patterns
	# do next test after dealing w/ empty patterns, because it creates empty hash values!!
	confess "$errstr. Patterns may not contain /" if $specials{$special}=~/\//;
      }
      push(@inputs,$in);
      my $out;
      if ($specials{'=>'}) {
	$renames->{$in}=1;
	$out=clean_path($specials{'=>'});
      } else {
	my $in_resource=$pipeline->get_resource($in);
	$out=file($in_resource->relative_id);
      }
      $input2out->{$in}=$out;
      $self->input2patterns($in,'ls_pattern',$specials{'=*'}) if length $specials{'=*'};
      $self->input2patterns($in,'perl_pattern',$specials{'=~'}) if length $specials{'=~'};
    } else {			# split line into words, each is 'in'. 'out' is file component
      my @ins=map {clean_path($_)} split;
      for my $in (@ins) {
	confess "Error in step ".$self->id.". Input file $in appears more than once"
	  if $input2out->{$in};
	# NG 11-01-06. By way of explanation, the following 2 lines seem to be a complicated
	#   way of getting the final component from a path.  It is almost equivalent to
	#   $input2out->{$in}=file($in);
	#   The only case in which it's different is if $in is a Namespace, in which case
	#   the result is empty.  I wonder if this is intentional...
	my $in_resource=$pipeline->get_resource($in);
	$input2out->{$in}=file($in_resource->relative_id);
      }
      push(@inputs,@ins);
    }}
  @inputs;
}
# check output paths. create if necessary and possible
# outupt is a Version
sub out_dir {
  my($self,$op)=@_;
  Data::Workflow::Util::out_dir($op->out_path,$op->create_paths);
}
# tests whether list of copy::Operations for a given input contains patterns.
# also checks for consistency.  all ops or none must have patterns, since pattern 
# implies input is directory
sub has_pattern {
  my($ops)=@_;
  my $has_pattern=grep {$_->patterns} @$ops;
  if ($has_pattern && $has_pattern!=@$ops) {
    my $checkstr=join('; ',map {'step='.$_->step_id.', input='.$_->input_id} @$ops);
    confess "Inconsistent use of patterns for file ".$_->input_id.": all operations must have pattern or none may, since the presence of a pattern implies the input is a directory.  Check $checkstr";
  }
  $has_pattern;
}

1;
