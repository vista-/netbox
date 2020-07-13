from django.apps import apps
from django.core.management.base import BaseCommand, CommandError

from circuits.models import CircuitTermination
from dcim.choices import CableStatusChoices
from dcim.models import CableTermination, FrontPort, RearPort


class Command(BaseCommand):
    help = "Recalculate connected endpoints for the specified models"

    def add_arguments(self, parser):
        parser.add_argument(
            'args', metavar='app_label.ModelName', nargs='*',
            help='One or more specific models (each prefixed with its app_label) to retrace',
        )

    def _get_models(self, names):
        """
        Compile a list of models to be retraced. If no names are specified, all models which have a connected endpoint
        will be included.
        """
        models = []

        if names:
            # Collect all NaturalOrderingFields present on the specified models
            for name in names:
                try:
                    app_label, model_name = name.split('.')
                except ValueError:
                    raise CommandError(
                        "Invalid format: {}. Models must be specified in the form app_label.ModelName.".format(name)
                    )
                try:
                    app_config = apps.get_app_config(app_label)
                except LookupError as e:
                    raise CommandError(str(e))
                try:
                    model = app_config.get_model(model_name)
                except LookupError:
                    raise CommandError("Unknown model: {}.{}".format(app_label, model_name))

                if not issubclass(model, CableTermination) or not hasattr(model, 'connected_endpoint'):
                    raise CommandError(
                        "Invalid model: {}.{} does not have a connected endpoint".format(app_label, model_name)
                    )
                models.append(model)

        else:
            # Find *all* models with NaturalOrderingFields
            for app_config in apps.get_app_configs():
                for model in app_config.models.values():
                    if issubclass(model, CableTermination) and hasattr(model, 'connected_endpoint'):
                        models.append(model)

        return models

    def handle(self, *args, verbosity, **options):

        models = self._get_models(args)

        if verbosity >= 1:
            self.stdout.write("Retracing {} models.".format(len(models)))

        for model in models:
            # Print the model and field name
            if verbosity >= 1:
                self.stdout.write(f"{model._meta.label}...", )
                self.stdout.flush()

            count = 0

            # Update any endpoints for this Cable.
            endpoints = model.objects.all()
            for endpoint in endpoints:
                path, split_ends, position_stack = endpoint.trace()
                # Determine overall path status (connected or planned)
                path_status = True
                for segment in path:
                    if segment[1] is None or segment[1].status != CableStatusChoices.STATUS_CONNECTED:
                        path_status = False
                        break

                endpoint_a = path[0][0]
                if not split_ends and not position_stack:
                    endpoint_b = path[-1][2]
                    if endpoint_b is None and len(path) >= 2 and isinstance(path[-2][2], CircuitTermination):
                        # Simulate the previous behaviour and use the circuit termination as connected endpoint
                        endpoint_b = path[-2][2]
                else:
                    endpoint_b = None

                # Patch panel ports are not connected endpoints, all other cable terminations are
                if isinstance(endpoint_a, CableTermination) and not isinstance(endpoint_a, (FrontPort, RearPort)) and \
                        isinstance(endpoint_b, CableTermination) and not isinstance(endpoint_b, (FrontPort, RearPort)):
                    if verbosity >= 3:
                        self.stdout.write(f"Updating path endpoints: "
                                          f"{endpoint_a.parent} {endpoint_a} <-> {endpoint_b.parent} {endpoint_b}")
                        self.stdout.flush()

                    endpoint_a.connected_endpoint = endpoint_b
                    endpoint_a.connection_status = path_status
                    endpoint_a.save()
                    endpoint_b.connected_endpoint = endpoint_a
                    endpoint_b.connection_status = path_status
                    endpoint_b.save()
                elif endpoint_b is None:
                    if verbosity >= 3:
                        self.stdout.write(f"Clearing path endpoint: {endpoint_a.parent} {endpoint_a}")
                        self.stdout.flush()

                    # There is no endpoint, so clean up any left overs
                    endpoint_a.connected_endpoint = None
                    endpoint_a.connection_status = path_status
                    endpoint_a.save()

                count += 1

            # Print the total count of alterations for the field
            if verbosity >= 2:
                self.stdout.write(self.style.SUCCESS(f"{count} {model._meta.verbose_name_plural} updated"))
            elif verbosity >= 1:
                self.stdout.write(self.style.SUCCESS(str(count)))

        if verbosity >= 1:
            self.stdout.write(self.style.SUCCESS("Done."))
