package Data::Workflow::Log::Object;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-12-30
# $Id: 
#
# Helper class for Data::Workflow::Log.
# Log messages to file. string, or handle
# Inspired by and includes code from many CPAN modules including File::Log,
# Log::StdLog, Log::Dispatch::Output, ::Handle, ::File::Locked, ::Stamped,
# ::Rotate, and IO::Automatic
#
#################################################################################

use strict;
use Class::AutoClass;
use Carp;
use IO::File;
#use IO::Scalar;
use POSIX qw(strftime);

use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES %SYNONYMS %DEFAULTS %AUTODB);
@ISA = qw(Class::AutoClass);

@AUTO_ATTRIBUTES=qw(id tie
		    stamp_fmt log_stamp log_nl log_level
		    file filename glob string handle mode perms
		    _min_level _max_level _print_level
		    _redirect_stderr saved_stderr opened
		    verbose);
@OTHER_ATTRIBUTES=qw(redirect_stderr min_level max_level print_level);
%SYNONYMS=(name=>'id',
	  'dest'=>'file',destination=>'file',);
%DEFAULTS=(id=>'Default Log',
	   min_level=>'debug',max_level=>'emergency',print_level=>'info',
	   log_stamp=>1,log_nl=>1,log_level=>0,
	   mode=>'a');
Class::AutoClass::declare(__PACKAGE__);

# code adapted from Log::Dispatch and Log::Dispatch::Output
our(%LEVEL2NUM,%NUM2LEVEL);

my @levels=qw(debug info notice warning error critical alert emergency);
map {$NUM2LEVEL{$_}=$levels[$_]} (0..$#levels);
my $i=0; map {$LEVEL2NUM{$_}=$i++} @levels;
# add in extra ones
$LEVEL2NUM{err}=$LEVEL2NUM{error};
$LEVEL2NUM{crit}=$LEVEL2NUM{critical};
$LEVEL2NUM{emerg}=$LEVEL2NUM{emergency};
# make 'level' methods
{no strict 'refs';
 for my $level (@levels,qw(err crit emerg)) {
   *{$level} = sub {my $self=shift;$self->log(level=>$level,message=>"@_");};
 }}

sub _init_self {
  my ($self, $class, $args) = @_;
  return unless $class eq __PACKAGE__;
 # open if possible. all args except autoflush already set
  if (exists $args->{autoflush}) {
    $self->open(autoflush=>$args->autoflush);
  } else {
    $self->open();
  }
}
# Arg processing
# (1) if a more specific file arg is set, use it as given...
# (2) if @_ set, use as a -file arg and process as below
# string: basically a filename, but may include mode ops
#         note: not easy to determine if mode ops present!
#         look for mode ops. if found, send string to IO::File::open
#         else, send string, mode, perms (if defined)
# string ref: 
#         send to IO::Scalar
# glob or glob ref: 
#         if it's opened, use as is (and hope for the best!!)
#         if not opened (fileno undef), error
# IO::Handle or other object
#         use as is. will work fine if really IO::Handle :)
sub open {
  my $self=shift;
  @_=(-file=>$_[0]) if @_==1 and 'HASH' ne ref $_[0];
  # NG 10-07-30: update to Hash::AutoHash::Args
  # my $args=new Class::AutoClass::Args(@_);
  my $args=new Hash::AutoHash::Args(@_);
  $self->set($args);
  my $autoflush=$args->{autoflush};		  # save for later
  my @file_attrs=qw(file filename glob string handle);
#  my $args=new Class::AutoClass::Args(@_);
#  # fill in defaults from object
#  for my $attr (@file_attrs,qw(mode perms redirect_stderr)) {
#    my $default=$self->$attr;
#    $args->$attr($default) unless defined $args->$attr() || !defined $default;
#  }
  # get params from arg list
#  my($file,$filename,$glob,$string_ref,$handle,$mode,$perms)=
#    $args->get_args(@file_attrs,qw(mode perms redirect_stderr));
  my($file,$filename,$glob,$string_ref,$handle,$mode,$perms)=
    $self->get(@file_attrs,qw(mode perms redirect_stderr));
  confess "At most one of ".join(', ',@file_attrs)." may be set" if 
    (grep {defined $_} ($file,$filename,$glob,$string_ref,$handle)) > 1;
  my($file_special,$glob_ref);
  if ($glob) {
    $glob_ref='GLOB' eq ref $glob? $glob: \$glob;
  } elsif ($file) {		      # case (2). figure out arg type
    if ('SCALAR' eq ref \$file) {     # string
      $_=$file;  		      # copy string so s/// ops won't munge
      s/^\s+|\s+$//g;		      # clean white space
      $file_special=/^(<|>|\+<|\+>|\|)|(\|)$/;
      $filename=$file unless $file_special;
    } elsif ('SCALAR' eq ref $file) { # string ref
      $string_ref=$file;
    } elsif ('GLOB' eq ref \$file) {  # GLOB
      $glob_ref=\$file;
    } elsif ('GLOB' eq ref $file) {   # GLOB ref
      $glob_ref=$file;
    } elsif (ref $file && UNIVERSAL::can($file,'print')) {
      $handle=$file;
    }
  }
  if ($file_special) {
    open($handle,$file) || confess "Cannot 'open' $file: $!";
  } elsif ($filename) {
    $mode='>' if $mode=~/w/;
    $mode='>>' if $mode=~/a/;
    open($handle,$mode,$filename) || confess "Cannot 'open' $filename: $!";
  } elsif ($string_ref) {
    $mode=~s/^\s+|\s+$//g;	                         # clean whitespace
    $$string_ref='' if grep {$mode eq $_} qw(w w+ > +>); # empty string if mode is 'write'
    open($handle,'>>',$string_ref) || confess "Cannot 'open' handle to string: $!";
  } elsif ($glob_ref) {
    confess "Unopened filehandle $$glob_ref passed to Data::Workflow::Log"
      unless fileno($glob_ref);
    $handle=$glob_ref;
  } elsif ($handle && !UNIVERSAL::can($handle,'print')) {
    confess "Unusable handle passed to Data::Workflow::Log: not object or can't print";
  } 
  $self->handle($handle);
  $self->opened(1);
  $self->autoflush(defined $autoflush? $autoflush: 1); # turn on autoflush by default
  $self->redirect_stderr;			  # redirects if parameter previously set
}
sub inc_opened {
  my $self=shift;
  my $opened=$self->opened;
  $self->opened($opened+1);
}
sub close {
  my $self=shift;
  my $handle=$self->handle;
  my $opened=$self->opened;
  if ($opened) {
    $opened--;
    $self->opened($opened);
    unless ($opened) {
      $self->handle(undef);
      close $handle;
    }}
}
sub print {
  my $self = shift;
  my $handle=$self->check_handle('print'); # just for error reporting
  $self->log(-level=>$self->print_level,-message=>join('',@_));
}
sub printf {			# code from IO:Scalar
  shift->print(sprintf(shift,@_));
}
sub write {			# based on code from Tie::StdHandle
  shift->print(substr(shift,0,shift));
}
# usage: log (-level=>$level, -message=>$message)
#    or  log ($level=>$message)
#    or  log ($message)
sub log {
  my $self=shift;
  my($level,$message);
  if (@_==1) {
    $level=$self->print_level;
    $message=shift;
  } else {
    # NG 10-07-30: update to Hash::AutoHash::Args
    # my $args=new Class::AutoClass::Args(@_);
    my $args=new Hash::AutoHash::Args(@_);
    ($level,$message)=@$args{qw(level message)};
    if (keys %$args==1 && !defined $level && !defined $message) { # maybe 2nd format
      ($level,$message)=@_;
    }
    $level=$self->print_level unless defined $level;
  }
  $level=$self->level_as_number($level);
  if ($level>=$self->min_level) { # proceed to print the message
    my @prefixes;
    if ($self->log_stamp) {
      my $stamp_fmt=$self->stamp_fmt;
      my $stamp=$stamp_fmt? strftime($stamp_fmt,localtime): scalar localtime;
      push(@prefixes,$stamp);
    }
    if ($self->log_level) {
      my $name=$self->level_as_name($level);
      push(@prefixes,"$name $level");
    }
    @prefixes=map {"[$_]"} @prefixes;
    $message=join(" ",@prefixes,$message);
    $message.="\n" if $self->log_nl && $message!~/\n$/;
    my $handle=$self->check_handle('log');
    print $handle $message;
  }
}
*printlog=\&log;
sub tell {
  my $self=shift;
  my $handle=$self->check_handle('tell');
  seek($handle,0,SEEK_END);	# seems necessary on 'strange' handles, eg, /dev/null
  tell $handle;
}

sub autoflush {
  my $self=shift;
  @_=(1) unless @_;		# default is 1
  my $handle=$self->handle;	# let handle do it if it can
  return undef unless $handle;
  return $handle->autoflush(@_) if UNIVERSAL::can($handle,'autoflush');
  if ('GLOB' eq ref $handle) {	# if GLOB ref, have to set autoflush ($|) variable
    my $old_handle=select;	# save currently selected handled
    select($handle);		# make $handle currently selected
    $|=$_[0];			# set variable
    my $autoflush=$|;		# read it back so it can be returned
    select($old_handle);	# restore selected handle
    return $autoflush;
  }
}
sub flush {
  my $self=shift;
  my $handle=$self->handle;	# let handle do it if it can
  return undef unless $handle;
  return $handle->flush if UNIVERSAL::can($handle,'flush');
  seek($handle,tell $handle,SEEK_SET); # this seems to work for GLOBs..
}
sub redirect_stderr {
  my $self=shift; 
  my $redirect_stderr=@_? 
    $self->_redirect_stderr($self->_redirect_stderr(shift)): $self->_redirect_stderr;
  if ($redirect_stderr && $self->opened) {
    open my $saved_stderr,">&STDERR" or confess "Can't save STDERR: $!";
    $self->saved_stderr($saved_stderr);
    $self->tie->dup(*STDERR);
  }
}
sub restore_stderr {
  my($self)=@_;
  my $saved_stderr=$self->saved_stderr;
  if ($saved_stderr) {
    close STDERR;
    untie *STDERR;
    open STDERR,'>&',$saved_stderr or confess "Can't restore STDERR: $!";
    $self->saved_stderr(undef);
  }
}
sub min_level {
  my $self=shift;
  my $min_level=@_? $self->_min_level($self->level_as_number(shift)): $self->_min_level;
  $min_level;
}
sub max_level {
  my $self=shift;
  my $max_level=@_? $self->_max_level($self->level_as_number(shift)): $self->_max_level;
  $max_level;
}
sub print_level {
  my $self=shift;
  my $print_level=@_? 
    $self->_print_level($self->level_as_number(shift)): $self->_print_level;
  $print_level;
}
sub level_as_number {
  my($self,$level)=@_;
  defined $level or $level=0;
  return $level if $level =~ /^\d$/;
  my $number=$LEVEL2NUM{$level};
  confess "$level is not a valid log level" unless defined $number;
  $number;
}
sub level_as_name {
  my($self,$level)=@_;
  defined $level or $level=0;
  return $level if defined $LEVEL2NUM{$level};
  my $name=$NUM2LEVEL{$level};
  confess "$level is not a valid log level" unless defined $name;
  $name;
}

sub check_handle {
  my $handle=shift->handle;
  my $method=shift;
  $method or $method='an IO::Handle operation';
  confess "Can't execute $method: handle not open" unless $handle;
  $handle;
}

1;
