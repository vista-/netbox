import sys

from django.core.exceptions import ObjectDoesNotExist
from django.db import migrations

# This migration contains copies of code as it was at the time that this migration was written. This makes sure that
# when this migration is run later it will not cause errors if that code has changed in the mean time. We can also not
# use isinstance() because migrations create fake classes for models, so they will not be instances of the real class
# (if that class even exists at the time the migration is run). So there is a lot of code duplication in here, for good
# reasons.


ENDPOINTS = (
    'ConsolePort',
    'ConsoleServerPort',
    'PowerPort',
    'PowerOutlet',
    'PowerFeed',
    'Interface',
    'CircuitTermination',
)


class CableTraceSplit(Exception):
    """
    A cable trace cannot be completed because a RearPort maps to multiple FrontPorts and
    we don't know which one to follow.
    """

    def __init__(self, termination, *_args, **_kwargs):
        self.termination = termination


def retrace(apps, schema_editor):
    contenttype_class = apps.get_model('contenttypes', 'ContentType')
    cable_class = apps.get_model('dcim', 'Cable')

    frontport_class = apps.get_model('dcim', 'FrontPort')
    rearport_class = apps.get_model('dcim', 'RearPort')

    consoleport_class = apps.get_model('dcim', 'ConsolePort')
    consoleserverport_class = apps.get_model('dcim', 'ConsoleServerPort')
    powerport_class = apps.get_model('dcim', 'PowerPort')
    poweroutlet_class = apps.get_model('dcim', 'PowerOutlet')
    powerfeed_class = apps.get_model('dcim', 'PowerFeed')
    interface_class = apps.get_model('dcim', 'Interface')
    circuittermination_class = apps.get_model('circuits', 'CircuitTermination')

    db_alias = schema_editor.connection.alias

    # noinspection PyProtectedMember
    def get_connected_endpoint(cable_termination):
        """
        Because migration models don't have methods we have to reimplement them here.
        """
        if cable_termination is None:
            return None

        elif cable_termination.__class__.__name__ in ('ConsolePort', 'PowerFeed', 'CircuitTermination'):
            # These just have their own connected_endpoint field
            return cable_termination.connected_endpoint

        elif cable_termination.__class__.__name__ == 'ConsoleServerPort':
            # A ConsoleServerPort connected_endpoint is a reverse OneToOne on a ConsolePort
            return consoleport_class.objects.filter(connected_endpoint=cable_termination).first()

        elif cable_termination.__class__.__name__ == 'PowerOutlet':
            # A PowerOutlet connected_endpoint is a reverse OneToOne on a PowerPort
            return powerport_class.objects.filter(_connected_poweroutlet=cable_termination).first()

        elif cable_termination.__class__.__name__ == 'PowerPort':
            # Duplicate the PowerPort connected_endpoint getter at the time of writing this migration
            try:
                if cable_termination._connected_poweroutlet:
                    return cable_termination._connected_poweroutlet
            except ObjectDoesNotExist:
                pass

            try:
                if cable_termination._connected_powerfeed:
                    return cable_termination._connected_powerfeed
            except ObjectDoesNotExist:
                pass

            return None

        elif cable_termination.__class__.__name__ == 'Interface':
            # Duplicate the Interface connected_endpoint getter at the time of writing this migration
            try:
                if cable_termination._connected_interface:
                    return cable_termination._connected_interface
            except ObjectDoesNotExist:
                pass

            try:
                if cable_termination._connected_circuittermination:
                    return cable_termination._connected_circuittermination
            except ObjectDoesNotExist:
                pass

            return None

        return None

    # noinspection PyProtectedMember
    def set_connected_endpoint(cable_termination, new_endpoint):
        if cable_termination is None:
            return

        elif cable_termination.__class__.__name__ in ('ConsolePort', 'PowerOutlet', 'CircuitTermination'):
            # These just have their own connected_endpoint field
            cable_termination.connected_endpoint = new_endpoint

        elif cable_termination.__class__.__name__ == 'ConsoleServerPort':
            # A ConsoleServerPort connected_endpoint is a reverse OneToOne on a ConsolePort
            # Don't do anything here, the other endpoint will set it
            pass

        elif cable_termination.__class__.__name__ == 'PowerOutlet':
            # A PowerOutlet connected_endpoint is a reverse OneToOne on a PowerPort
            # Don't do anything here, the other endpoint will set it
            pass

        elif cable_termination.__class__.__name__ == 'PowerPort':
            # Duplicate the PowerPort connected_endpoint setter at the time of writing this migration
            if new_endpoint.__class__.__name__ == 'PowerOutlet':
                cable_termination._connected_poweroutlet = new_endpoint
                cable_termination._connected_powerfeed = None
            elif new_endpoint.__class__.__name__ == 'PowerFeed':
                cable_termination._connected_poweroutlet = None
                cable_termination._connected_powerfeed = new_endpoint
            else:
                cable_termination._connected_poweroutlet = None
                cable_termination._connected_powerfeed = None

        elif cable_termination.__class__.__name__ == 'Interface':
            # Duplicate the Interface connected_endpoint setter at the time of writing this migration
            if new_endpoint.__class__.__name__ == 'Interface':
                cable_termination._connected_interface = new_endpoint
                cable_termination._connected_circuittermination = None
            elif new_endpoint.__class__.__name__ == 'CircuitTermination':
                cable_termination._connected_interface = None
                cable_termination._connected_circuittermination = new_endpoint
            else:
                cable_termination._connected_interface = None
                cable_termination._connected_circuittermination = None

    def trace(cable_termination):
        """
        Return three items: the traceable portion of a cable path, the termination points where it splits (if any), and
        the remaining positions on the position stack (if any). Splits occur when the trace is initiated from a midpoint
        along a path which traverses a RearPort. In cases where the originating endpoint is unknown, it is not possible
        to know which corresponding FrontPort to follow. Remaining positions occur when tracing a path that traverses
        a FrontPort without traversing a RearPort again.

        The path is a list representing a complete cable path, with each individual segment represented as a
        three-tuple:

            [
                (termination A, cable, termination B),
                (termination C, cable, termination D),
                (termination E, cable, termination F)
            ]
        """
        trace_endpoint = cable_termination
        trace_path = []
        trace_position_stack = []

        def get_peer_port(termination):
            # Map a front port to its corresponding rear port
            if termination.__class__.__name__ == 'FrontPort':
                # Retrieve the corresponding RearPort from database to ensure we have an up-to-date instance
                peer_port = rearport_class.objects.get(pk=termination.rear_port.pk)

                # Don't use the stack for RearPorts with a single position. Only remember the position at
                # many-to-one points so we can select the correct FrontPort when we reach the corresponding
                # one-to-many point.
                if peer_port.positions > 1:
                    trace_position_stack.append(termination)

                return peer_port

            # Map a rear port/position to its corresponding front port
            elif termination.__class__.__name__ == 'RearPort':
                if termination.positions > 1:
                    # Can't map to a FrontPort without a position if there are multiple options
                    if not trace_position_stack:
                        raise CableTraceSplit(termination)

                    front_port = trace_position_stack.pop()
                    position = front_port.rear_port_position

                    # Validate the position
                    if position not in range(1, termination.positions + 1):
                        raise Exception("Invalid position for {} ({} positions): {})".format(
                            termination, termination.positions, position
                        ))
                else:
                    # Don't use the stack for RearPorts with a single position. The only possible position is 1.
                    position = 1

                try:
                    peer_port = frontport_class.objects.get(
                        rear_port=termination,
                        rear_port_position=position,
                    )
                    return peer_port
                except frontport_class.ObjectDoesNotExist:
                    return None

            # Follow a circuit to its other termination
            elif termination.__class__.__name__ == 'CircuitTermination':
                peer_side = 'Z' if termination.term_side == 'A' else 'A'
                try:
                    peer_termination = circuittermination_class.objects.prefetch_related('site').get(
                        circuit=termination.circuit,
                        term_side=peer_side
                    )
                except circuittermination_class.DoesNotExist:
                    return None

                return peer_termination

            # Termination is not a pass-through port
            else:
                return None

        while trace_endpoint is not None:
            # No cable connected; nothing to trace
            if not trace_endpoint.cable:
                trace_path.append((trace_endpoint, None, None))
                return trace_path, None, trace_position_stack

            # Check for loops
            if trace_endpoint.cable in [my_segment[1] for my_segment in trace_path]:
                return trace_path, None, trace_position_stack

            # Record the current segment in the path
            # noinspection PyProtectedMember
            endpoint_ct = contenttype_class.objects.get(app_label=trace_endpoint._meta.app_label,
                                                        model=trace_endpoint._meta.model_name)
            cable = cable_class.objects.filter(termination_a_type=endpoint_ct,
                                               termination_a_id=trace_endpoint.pk).first()
            if cable:
                termination_class = apps.get_model(cable.termination_b_type.app_label,
                                                   cable.termination_b_type.model)
                far_end = termination_class.objects.get(pk=cable.termination_b_id)
            else:
                cable = cable_class.objects.filter(termination_b_type=endpoint_ct,
                                                   termination_b_id=trace_endpoint.pk).first()
                if cable:
                    termination_class = apps.get_model(cable.termination_a_type.app_label,
                                                       cable.termination_a_type.model)
                    far_end = termination_class.objects.get(pk=cable.termination_a_id)
                else:
                    far_end = None

            trace_path.append((trace_endpoint, trace_endpoint.cable, far_end))

            # Get the peer port of the far end termination
            try:
                trace_endpoint = get_peer_port(far_end)
            except CableTraceSplit as e:
                return trace_path, e.termination.frontports.all(), trace_position_stack

            if trace_endpoint is None:
                return trace_path, None, trace_position_stack

    def endpoints():
        yield from consoleport_class.objects.using(db_alias).all()
        yield from consoleserverport_class.objects.using(db_alias).all()
        yield from powerport_class.objects.using(db_alias).all()
        yield from poweroutlet_class.objects.using(db_alias).all()
        yield from powerfeed_class.objects.using(db_alias).all()
        yield from interface_class.objects.using(db_alias).all()
        yield from circuittermination_class.objects.using(db_alias).all()

    i = 0
    for endpoint in endpoints():
        if 'test' not in sys.argv:
            if i % 100 == 0:
                print('.', end='', flush=True)
            i += 1

        path, split_ends, position_stack = trace(endpoint)

        # Determine overall path status (connected or planned)
        path_status = True
        for segment in path:
            if segment[1] is None or segment[1].status != 'connected':
                path_status = False
                break

        endpoint_a = path[0][0]
        if not split_ends and not position_stack:
            endpoint_b = path[-1][2]
            if endpoint_b is None and len(path) >= 2 and path[-2][2].__class__.__name__ == 'CircuitTermination':
                # Simulate the previous behaviour and use the circuit termination as connected endpoint
                endpoint_b = path[-2][2]
        else:
            endpoint_b = None

        # Patch panel ports are not connected endpoints, all other cable terminations are
        current_endpoint = get_connected_endpoint(endpoint_a)
        if endpoint_a.__class__.__name__ in ENDPOINTS and \
                (endpoint_b is None or endpoint_b.__class__.__name__ in ENDPOINTS):
            if current_endpoint != endpoint_b or endpoint_a.connection_status != path_status:
                set_connected_endpoint(endpoint_a, endpoint_b)
                endpoint_a.connection_status = path_status
                endpoint_a.save()


class Migration(migrations.Migration):
    dependencies = [
        ('dcim', '0114_update_jsonfield'),
    ]

    operations = [
        migrations.RunPython(retrace, migrations.RunPython.noop)
    ]
