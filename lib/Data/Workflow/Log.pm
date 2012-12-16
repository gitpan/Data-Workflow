package Data::Workflow::Log;

#################################################################################
#
# Author:  Nat Goodman
# Created: 05-12-30
# $Id: 
#
# Log messages to file. string, or handle
# Inspired by and includes code from many CPAN modules including File::Log,
# Log::StdLog, Log::Dispatch::Output, ::Handle, ::File::Locked, ::Stamped,
# ::Rotate, and IO::Automatic
#
#################################################################################

use strict;
use Carp;
use Symbol;
use Data::Workflow::Log::Object;

# return null log
our $null_log=new Data::Workflow::Log(-file=>'/dev/null',-mode=>'a');
sub null {$null_log;}

sub new {
  my $class = shift;
  $class = ref($class) || $class || __PACKAGE__;
  my $self=bless gensym, $class;
  tie *$self, $class, $self;
  # if there's one arg and it's undef, return null_log
  return null if @_==1 && !defined $_[0];
  # if there's one arg and it's not a HASH, assume it's a destination
  @_=(-file=>$_[0]) if @_==1 and 'HASH' ne ref $_[0];
  my $helper=new Data::Workflow::Log::Object(@_,-tie=>$self);
  $ {*$self}{_helper}=$helper;
  $self;
}
sub helper {$ {*{$_[0]}}{_helper};}
sub handle {$_[0]->helper->handle;}
sub check_handle {shift->helper->check_handle(@_);}
sub dup {
  my($self,$fh)=@_;
  my $class=ref $self;
  my $helper=$self->helper;
  confess "Can't dup: Data::Workflow::Log object is not open" unless $helper->opened;
  $helper->inc_opened;
  tie $fh,$class,$self;
}

# Need entire interface...
sub TIEHANDLE {
  ((defined($_[1]) && UNIVERSAL::isa($_[1], __PACKAGE__))
     ? $_[1]
     : shift->new(@_));
}

# These are processed by helper.
sub OPEN {shift->helper->open(@_);}
sub open {shift->helper->open(@_);}
sub PRINT {shift->helper->print(@_);}
sub print {shift->helper->print(@_);}
sub PRINTF {shift->helper->printf(@_);}
sub printf {shift->helper->print(sprintf(shift,@_));}
sub WRITE {shift->helper->write(@_);}
sub TELL {shift->helper->tell(@_);}
sub CLOSE {shift->helper->close(@_);}
#sub UNTIE {shift->helper->close(@_);}

sub printlog {shift->helper->log(@_);}
my @levels=qw(debug info notice warning error critical alert emergency);
{no strict 'refs';
 for my $level (@levels,qw(err crit emerg)) {
   *{$level} = sub {my $self=shift;$self->log(level=>$level,message=>"@_");};
 }}

## These are processed by handle. nop if not set.
#sub CLOSE {my $handle=shift->handle; return ref $handle? $handle->close(@_): undef;}
#sub UNTIE {my $handle=shift->handle; return ref $handle? $handle->close(@_): undef;}

# Logs do not support input operations
sub _no_input_ops {confess "Input operations not supported by package \"".__PACKAGE__."\"";}
sub GETC {shift->_no_input_ops;}
sub READ {shift->_no_input_ops;}
sub READLINE {shift->_no_input_ops;}

# These are processed by handle. error if not set
sub BINMODE {binmode(shift->check_handle('binmode'));}
sub SEEK {seek(shift->check_handle('seek'),shift,shift);}
sub EOF {eof(shift->check_handle('eof'));}
sub FILENO {fileno(shift->check_handle('fileno'));}
sub binmode {binmode(shift->check_handle('binmode'));}
sub seek {seek(shift->check_handle('seek'),shift,shift);}
sub eof {eof(shift->check_handle('eof'));}
sub fileno {fileno(shift->check_handle('fileno'));}

use vars qw($AUTOLOAD);

sub AUTOLOAD {
  my $self=shift;
  my $method=$AUTOLOAD;
  $method=~s/^.*:://;             # strip class qualification
  return if $method eq 'DESTROY'; # the books say you should do this
  my $helper=$self->helper;	  # let helper do it if it can
  return $helper->$method(@_) if $helper->can($method);
  my $handle=$self->handle;	  # last gasp: let handle try
  return $handle->$method(@_) if UNIVERSAL::can($handle,$method);
  confess "Can't locate object method $method via package \"".__PACKAGE__."\"";
}

1;
