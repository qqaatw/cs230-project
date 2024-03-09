from argparse import ArgumentParser

import torch
import torch.nn as nn
import torch.optim as optim
import torchvision
import torchvision.transforms as transforms
from torchvision.models import resnet18, resnet34, resnet50
import time

from cs230_common.sdk import SDK
from cs230_common.utils import get_general_parser, get_hyperparameters_parser

BROKER_HOST = "18.119.97.104"
BROKER_PORT = "5673"

model_map = {
    "resnet18": resnet18,
    "resnet34": resnet34,
    "resnet50": resnet50,
}


def main(sdk, args):
    # Define device
    device = sdk.device()

    # MNIST dataset
    transform = transforms.Compose(
        [
            transforms.ToTensor(),
            transforms.Normalize(
                (0.1307,), (0.3081,)
            ),  # Mean and standard deviation of MNIST dataset
        ]
    )
    train_dataset = torchvision.datasets.MNIST(
        root="./data", train=True, transform=transform, download=True
    )
    test_dataset = torchvision.datasets.MNIST(
        root="./data", train=False, transform=transform
    )

    # Data loader
    train_loader = torch.utils.data.DataLoader(
        dataset=train_dataset, batch_size=args.batch_size, shuffle=True
    )
    test_loader = torch.utils.data.DataLoader(
        dataset=test_dataset, batch_size=args.batch_size, shuffle=False
    )

    # Load pre-trained ResNet-18 model
    model = model_map[args.model](pretrained=True)

    # Modify the first convolutional layer to accept single-channel input
    model.conv1 = nn.Conv2d(1, 64, kernel_size=7, stride=2, padding=3, bias=False)

    # Modify the last fully connected layer to have 10 output features (for 10 classes in MNIST)
    model.fc = nn.Linear(model.fc.in_features, args.num_classes)

    # Move the model to the device
    model = model.to(device)

    # Loss and optimizer
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=args.learning_rate)

    t = time.time()
    # Training loop
    total_steps = len(train_loader)
    for epoch in range(args.num_epochs):
        for i, (images, labels) in enumerate(train_loader):
            images = images.to(device)
            labels = labels.to(device)

            # Forward pass
            outputs = model(images)
            loss = criterion(outputs, labels)

            # Backward and optimize
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            if (i + 1) % 100 == 0:
                print(
                    "Epoch [{}/{}], Step [{}/{}], Loss: {:.4f}".format(
                        epoch + 1, args.num_epochs, i + 1, total_steps, loss.item()
                    )
                )

    # Test the model
    model.eval()  # Set the model to evaluation mode
    with torch.no_grad():
        correct = 0
        total = 0
        for images, labels in test_loader:
            images = images.to(device)
            labels = labels.to(device)
            outputs = model(images)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()

        print(
            "Accuracy of the network on the 10000 test images: {} %".format(
                100 * correct / total
            )
        )

    torch.save(model.state_dict(), "model.pt")
    sdk.report("model.pt", None)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="MNIST ResNet Training",
        parents=[get_general_parser(), get_hyperparameters_parser()],
    )
    args = parser.parse_args()

    sdk = SDK(
        args.account,
        args.password,
        ftp_host=BROKER_HOST,
        ftp_port=21,
        broker_host=BROKER_HOST,
        broker_port=BROKER_PORT,
        receive_topics=["scheduler_to_api"],
    )

    sdk.launch(sdk, main, args)
