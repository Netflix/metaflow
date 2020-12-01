from metaflow import FlowSpec, Parameter, step, resources, argo, argo_base


@argo_base(image='tensorflow/tensorflow:2.2.1-py3')
class MnistFlow(FlowSpec):

    epochs = Parameter('epochs',
                       help='number of epochs',
                       default=30)
    batch_size = Parameter('batch_size',
                           default=512)

    @step
    def start(self):
        """
        Load the dataset
        """
        import tensorflow as tf
        self.train, self.test = tf.keras.datasets.mnist.load_data('mnist.npz')
        self.next(self.preprocess_train_data, self.preprocess_test_data)


    @step
    def preprocess_train_data(self):
        """
        Pre-process the train data
        """
        self.train_images, self.train_labels = self.train
        print("Number of train samples: {}".format(len(self.train_images)))
        self.next(self.training)


    @step
    def preprocess_test_data(self):
        """
        Pre-process the test data
        """
        self.test_images, self.test_labels = self.test
        print("Number of test samples: {}".format(len(self.test_images)))
        self.next(self.evaluate)


    @argo(image='tensorflow/tensorflow:2.2.1-gpu-py3', nodeSelector={'gpu': 'nvidia-tesla-v100'})
    @resources(gpu=1, cpu=2, memory=6000)
    @step
    def training(self):
        """
        Train the model
        """
        import tensorflow as tf
        import h5py
        model = tf.keras.models.Sequential([
            tf.keras.layers.Flatten(input_shape=(28, 28)),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(10, activation='softmax')
        ])
        model.compile(optimizer='adam',
                      loss='sparse_categorical_crossentropy',
                      metrics=['accuracy'])
        model.fit(self.train_images,
                  self.train_labels,
                  batch_size=self.batch_size,
                  epochs=self.epochs)
        with h5py.File('trained_model', driver='core', backing_store=False) as model_h5:
            # save to memory rather than disk until artifacts are supported
            model.save(model_h5)
            model_h5.flush()
            self.model = model_h5.id.get_file_image()
        self.next(self.evaluate)


    @step
    def evaluate(self, inputs):
        """
        Evaluate the model
        """ 
        import tensorflow as tf
        import h5py
        import io
        h5 = h5py.File(io.BytesIO(inputs.training.model), 'r')
        model = tf.keras.models.load_model(h5)
        self.results = model.evaluate(inputs.preprocess_test_data.test_images,
                                      inputs.preprocess_test_data.test_labels)
        self.next(self.end)


    @argo(image='python:3-slim')
    @step
    def end(self):
        """
        Print results
        """
        print(self.results)


if __name__ == '__main__':
    MnistFlow()
