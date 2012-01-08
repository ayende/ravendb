﻿using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Navigation;
using Raven.Studio.Behaviors;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio
{
	public partial class MainPage : UserControl
	{
		public MainPage()
		{
			InitializeComponent();
		}

		private bool isCtrlDown;
		protected override void OnKeyUp(System.Windows.Input.KeyEventArgs e)
		{
			switch (e.Key)
			{
				case Key.Ctrl:
					isCtrlDown = false;
					break;
			}
		}

		protected override void OnKeyDown(KeyEventArgs e)
		{
			switch (e.Key)
			{
				case Key.O:
					if (isCtrlDown)
						new NavigateToDocumentByIdCommand().Execute(null);
					break;
				case Key.Ctrl:
					isCtrlDown = true;
					break;
			}
		}

		// After the Frame navigates, ensure the HyperlinkButton representing the current page is selected
		private void ContentFrame_Navigated(object sender, NavigationEventArgs e)
		{
			HighlightCurrentPage(e.Uri);
		}

		private void HighlightCurrentPage(Uri currentUri)
		{
			foreach (var hyperlink in MainLinks.Children.OfType<HyperlinkButton>())
			{
				if (HyperlinkMatchesUri(currentUri.ToString(), hyperlink))
				{
					VisualStateManager.GoToState(hyperlink, "ActiveLink", true);
				}
				else
				{
					VisualStateManager.GoToState(hyperlink, "InactiveLink", true);
				}
			}

			if (currentUri.ToString() == string.Empty)
			{
				VisualStateManager.GoToState(SummaryLink, "ActiveLink", true);
			}
		}

		private static bool HyperlinkMatchesUri(string uri, HyperlinkButton link)
		{
			if (link.CommandParameter != null && 
				uri.StartsWith(link.CommandParameter.ToString(), StringComparison.InvariantCultureIgnoreCase))
			{
				return true;
			}

			var alternativeUris = LinkHighlighter.GetAlternativeUris(link);
			if (alternativeUris != null && alternativeUris.Any(alternative => uri.StartsWith(alternative, StringComparison.InvariantCultureIgnoreCase)))
			{
				return true;
			}

			return false;
		}

		// If an error occurs during navigation, show an error window
		private void ContentFrame_NavigationFailed(object sender, NavigationFailedEventArgs e)
		{
			e.Handled = true;
			ErrorPresenter.Show(e.Exception);
		}
	}
}