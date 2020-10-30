from metaflow import FlowSpec, Parameter, step, resources, argo, argo_base


@argo_base(image='tensorflow/tensorflow:latest-gpu-py3')
class MnistFlow(FlowSpec):

    epochs = Parameter('epochs',
                       help='number of epochs',
                       default=30)

    batch_size = Parameter('batch_size',
                           default=512)

    @step
    def start(self):
        """
        Load data from data repository e.g. S3
        """
        import tensorflow as tf

        (self.train_images, self.train_labels), (self.test_images, self.test_labels) = \
            tf.keras.datasets.mnist.load_data('mnist.npz')

        self.next(self.preprocess_train_data, self.preprocess_test_data)

    @argo(image='clearlinux/numpy-mp')
    @step
    def preprocess_train_data(self):
        """
        option to preprocess e.g. normalize input data
        Todo:persist preprocessed data
        """
        self.images = self.train_images
        print("Number of train samples: {}".format(len(self.images)))

        self.next(self.train)

    @argo(image='clearlinux/numpy-mp')
    @step
    def preprocess_test_data(self):
        """
        option to preprocess e.g. normalize input data
        Todo:persist preprocessed data
        """
        self.images = self.test_images
        print("Number of test samples: {}".format(len(self.images)))

        self.next(self.train)

    @argo(nodeSelector={'gpu': 'nvidia-tesla-v100'})
    @resources(gpu=1, cpu=2, memory=6000)
    @step
    def train(self, inputs):
        """
        collect preprocessed data and use for training
        Todo: persist model
        """
        import tensorflow as tf
        import h5py

        train_images = inputs.preprocess_train_data.images
        test_images = inputs.preprocess_test_data.images
        self.merge_artifacts(inputs, exclude=['images', 'train_images', 'test_images'])

        self.test_images = test_images

        model = tf.keras.models.Sequential([
            tf.keras.layers.Flatten(input_shape=(28, 28)),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(10, activation='softmax')
        ])

        model.compile(optimizer='adam',
                      loss='sparse_categorical_crossentropy',
                      metrics=['accuracy'])

        model.fit(train_images, self.train_labels, batch_size=self.batch_size, epochs=self.epochs)

        with h5py.File('trained_model', driver='core', backing_store=False) as model_h5:
            # save to memory rather than disk until artifacts are supported
            model.save(model_h5)
            model_h5.flush()
            self.model = model_h5.id.get_file_image()

        self.next(self.end)

    @resources(gpu=1, cpu=2, memory=4000)
    @step
    def end(self):
        """
        evaluate model
        Todo: write metrics
        """
        import tensorflow as tf
        import h5py
        import io

        f = io.BytesIO(self.model)
        model_stream = h5py.File(f, 'r')
        model = tf.keras.models.load_model(model_stream)

        self.results = model.evaluate(self.test_images, self.test_labels)


if __name__ == '__main__':
    MnistFlow()
