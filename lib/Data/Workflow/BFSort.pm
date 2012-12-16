package Data::Workflow::BFSort;
use vars qw(@ISA @AUTO_ATTRIBUTES @OTHER_ATTRIBUTES %SYNONYMS %DEFAULTS);
use Class::AutoClass;
use Exporter qw(import);
use Carp;
use Graph;
use strict;
@ISA = qw(Class::AutoClass); # AutoClass must be first!!

use constant {TOPOSORT_ONE=>1,TOPOSORT_READY=>2,TOPOSORT_ALL=>999};
our @EXPORT=qw(TOPOSORT_ONE TOPOSORT_READY TOPOSORT_ALL);

@AUTO_ATTRIBUTES=qw(order graph mode is_initialized
		     past present future
		   _copy);
@OTHER_ATTRIBUTES=qw();
%SYNONYMS=();
%DEFAULTS=(order=>'topo',
	   mode=>TOPOSORT_READY,
	   past=>{},
	   future=>[]);
Class::AutoClass::declare(__PACKAGE__);

sub _init_self {
  my($self,$class,$args)=@_;
  return unless $class eq __PACKAGE__; # to prevent subclasses from re-running this
  my $graph=$self->graph || $self->graph(new Graph::Directed);
  $self->copy($graph);
}
sub copy {
  my $self=shift @_;
  @_? $self->_copy($_[0]->copy): $self->_copy;
}

sub has_next {
  my($self)=@_;
  $self->reset unless $self->is_initialized;
  @{$self->future}>0;
}
sub get_next {
  my $self=shift @_;
  my $mode=@_?  $_[0]: $self->mode;
  $self->reset unless $self->is_initialized;
  my($graph,$past,$future)=$self->get(qw(copy past future));
  if ($mode==TOPOSORT_ONE) {
    return undef unless @$future;
    my $present=shift @$future;
    $self->visit($graph,$past,$present,$future);
    return $present;
  } elsif ($mode==TOPOSORT_READY) {
    return wantarray? (): undef unless @$future;
    my $present=$future;
    $future=$self->future([]); 
    $self->visit($graph,$past,$present,$future);
    return wantarray? @$present: $present;
  } elsif ($mode==TOPOSORT_ALL) {
    my @toposort;
    while (@$future) {
      my $present=$future;
      $future=$self->future([]);
      push(@toposort,@$present);
      $self->visit($graph,$past,$present,$future);
    }
    return wantarray? @toposort: \@toposort;
  } else {
    confess "Invalid mode $mode: should TOPOSORT_ONE, TOPOSORT_READY, or TOPOSORT_ALL";
  }
}
sub get_all {
  $_[0]->reset;
  $_[0]->get_next(TOPOSORT_ALL);
}
sub get_this {
  my $self=shift @_;
  my $mode=@_?  $_[0]: $self->mode;
  $self->reset unless $self->is_initialized;
  # 'present' nodes are ones ready to be visited, if any
  # call get_next if no nodes are yet visited
  my $present=$self->present || $self->get_next($mode);	
  if ($mode==TOPOSORT_ONE) {
    return @$present? $present->[0]: undef;
  } elsif ($mode==TOPOSORT_READY) { 
    return undef unless @$present;
    return wantarray? @$present: $present;
  }  else {
    confess "Invalid mode $mode: should TOPOSORT_ONE, or TOPOSORT_READY";
  }
}

sub visit {
  my($self,$graph,$past,$present,$future)=@_;
  my @present=_flatten($present);
  @$past{@present}=(1)x@present; # mark new ones as 'old' 
  for my $present (@present) {
    # add succesors to future, if possible
    my @successors=$graph->successors($present);
    for my $successor (@successors) {           # algorithm:
      $graph->delete_edge($present,$successor);	# delete edge to successor, and
				                # add when in_degree reaches 0
      push(@$future,$successor) unless $graph->in_degree($successor);
    }
  }
  $self->present(\@present);	# used by get_this
}

sub reset {
  my($self)= @_;
  my $graph=$self->graph;
  $self->copy($graph);
  $self->past({});
  my @future=$graph->predecessorless_vertices;
  $self->present(undef);	# used in get_this
  $self->future(\@future);
  $self->is_initialized(1);
}

sub _flatten {map {'ARRAY' eq ref $_? @$_: $_} @_;}
1;
